/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.io.sstable;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Writes an sstable index containing blocks of compound entries, clustered by component.
 * The number of entries made in the index per row varies with width, but will never be
 * less than 1.
 */
class NestedIndexWriter extends IndexWriter
{
    // for each level, a type to be used for compression
    private final List<AbstractType> types;

    protected final List<List<ByteBuffer>> tuples;
    // a bitset per name level that toggles when the parent of that level has changed
    protected final List<Observer.IndexedBitSet> nameparents;
    // TODO: avoid boxing
    protected final List<Long> offsets;
    // we store a single level's worth of metadata for observed rows
    protected final List<Long> markedForDeleteAt;
    protected final List<Long> localDeletionTime;
    protected final Observer.IndexedBitSet metaparent;

    // a reusable buffer for output data, before it is appended to the file
    private ByteBuffer outbuff = ByteBuffer.allocate(4096);

    protected NestedIndexWriter(Descriptor desc, CFMetaData meta, IPartitioner part, ReplayPosition rp, long keyCount) throws IOException
    {
        super(desc, part, rp, keyCount);
        this.types = meta.getTypes();
        this.nameparents = new ArrayList<Observer.IndexedBitSet>();
        this.tuples = new ArrayList<List<ByteBuffer>>();
        this.tuples.add(new ArrayList<ByteBuffer>());
        for (int i = 1; i < this.types.size(); i++)
        {
            this.tuples.add(new ArrayList<ByteBuffer>());
            this.nameparents.add(new Observer.IndexedBitSet(1024));
        }
        this.offsets = new ArrayList<Long>();
        this.markedForDeleteAt = new ArrayList<Long>();
        this.localDeletionTime = new ArrayList<Long>();
        this.metaparent = new Observer.IndexedBitSet(1024);
    }

    /** @return An Observer to collect tuples from rows in the data file. */
    @Override
    public Observer observer()
    {
        return new Observer(types.size(),
                            16,
                            DatabaseDescriptor.getColumnIndexSize(),
                            DatabaseDescriptor.getColumnIndexSize() / 128)
        {
            /** Collect first, last, interval'th or size threshold'd. */
            public boolean shouldAdd(int depth, boolean last)
            {
                return last || count % countThreshold == 0 || size > sizeThreshold;
            }
        };
    }

    /**
     * TODO: this copying is necessary because SSTableWriter can be reset mid row:
     * the contents it observes per row need to go into a temporary buffer.
     */
    @Override
    protected void appendToIndex(Observer observer, long dataSize) throws IOException
    {
        assert observer.keys.size() == 1 : "Oddly sized row: " + observer;

        if (summary.shouldAddEntry())
        {
            DecoratedKey firstKey = observer.keys.get(0);
            // the summary would like to observe a value: flush the current block
            // so that the value to observe is the first in a new block
            this.flush();
            summary.addEntry(firstKey, indexFile.getFilePointer());
            if (logger.isTraceEnabled())
                logger.trace("recorded summary of index for " + firstKey + " at " + indexFile.getFilePointer());
        }
        summary.incrementRowid();

        // copy observed content into our storage
        for (DecoratedKey key : observer.keys)
            this.tuples.get(0).add(key.key);
        for (int i = 1; i < this.tuples.size(); i++)
        {
            this.nameparents.get(i - 1).copyFrom(observer.nameparents.get(i - 1));
            this.tuples.get(i).addAll(observer.names.get(i - 1));
        }
        this.offsets.addAll(observer.offsets);
        this.markedForDeleteAt.addAll(observer.markedForDeleteAt);
        this.localDeletionTime.addAll(observer.localDeletionTime);
        this.metaparent.copyFrom(observer.metaparent);
    }

    /**
     * Override flush to close the current block: this method is the inverse of NestedReader.readBlock.
     */
    @Override
    protected void flush() throws IOException
    {
        if (offsets.isEmpty())
            return;
        // prepare the buffer for reuse
        outbuff.clear();

        // mark the beginning of the block as a splittable position
        long start = indexFile.getFilePointer();
        builder.addPotentialBoundary(start);

        // encode each value using the appropriate AbstractType
        outbuff = types.get(0).compress(desc, tuples.get(0), outbuff);
        // record metadata (0 or 1 entry per key, depending on row width)
        outbuff = LongType.encode(metaparent.asLongCollection(), outbuff);
        outbuff = LongType.encode(new LongList(markedForDeleteAt), outbuff);
        outbuff = LongType.encode(new LongList(localDeletionTime), outbuff);
        for (int i = 1; i < types.size(); i++)
        {
            outbuff = LongType.encode(nameparents.get(i - 1).asLongCollection(), outbuff);
            outbuff = types.get(i).compress(desc, tuples.get(i), outbuff);
        }
        // encode the offsets using the LongType primitive
        outbuff = LongType.encode(new LongList(offsets), outbuff);

        // append the buffer
        outbuff.flip();
        ByteBufferUtil.writeWithLength(outbuff, indexFile);
        logger.debug("Wrote block of {} positions in {} bytes", offsets.size(), indexFile.getFilePointer() - start);
        clear();
    }

    private void clear()
    {
        for (int i = 0; i < tuples.size(); i++)
            tuples.get(i).clear();
        for (int i = 0; i < nameparents.size(); i++)
            nameparents.get(i).clear();
        metaparent.clear();
        offsets.clear();
        markedForDeleteAt.clear();
        localDeletionTime.clear();
    }

    // TODO: the point of LongCollection was to avoid boxing: need
    // an implementation of LongList that doesn't box
    private static final class LongList extends LongType.LongCollection
    {
        private final List<Long> list;
        public LongList(List<Long> list)
        {
            super(list.size());
            this.list = list;
        }
        
        public long get(int i)
        {
            return list.get(i);
        }
    }
}
