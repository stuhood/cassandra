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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.net.MessagingService;

/**
 * Writes an sstable index containing blocks of compound entries, clustered by component.
 * The number of entries made in the index per row varies with width, but will never be
 * less than 1.
 */
class NestedIndexWriter extends IndexWriter
{
    private static final int VERSION = MessagingService.version_;

    // for each level, a type to be used for compression
    private final List<AbstractType> types;

    protected final List<List<ByteBuffer>> tuples;
    // a bitset per level that toggles when the parent of that level has changed
    protected final List<Observer.IndexedBitSet> parents;
    // TODO: avoid boxing
    protected final List<Long> offsets;

    protected NestedIndexWriter(Descriptor desc, CFMetaData meta, IPartitioner part, long keyCount) throws IOException
    {
        super(desc, part, keyCount);
        this.types = meta.getTypes();
        this.parents = new ArrayList<Observer.IndexedBitSet>();
        this.tuples = new ArrayList<List<ByteBuffer>>();
        this.tuples.add(new ArrayList<ByteBuffer>());
        for (int i = 1; i < this.types.size(); i++)
        {
            this.tuples.add(new ArrayList<ByteBuffer>());
            this.parents.add(new Observer.IndexedBitSet(1024));
        }
        this.offsets = new ArrayList<Long>();
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

        // copy observed positions into our storage
        // TODO: this copying is necessary because SSTableWriter can be reset mid row,
        // so the contents it observes go into a temporary buffer
        for (DecoratedKey key : observer.keys)
            this.tuples.get(0).add(key.key);
        for (int i = 1; i < this.tuples.size(); i++)
        {
            this.parents.get(i - 1).copyFrom(observer.parents.get(i - 1));
            this.tuples.get(i).addAll(observer.names.get(i - 1));
        }
        this.offsets.addAll(observer.offsets);
    }

    /**
     * Override flush to close the current block: this method is the inverse of NestedReader.readBlock.
     */
    @Override
    protected void flush() throws IOException
    {
        if (offsets.isEmpty())
            return;

        // mark the beginning of the block as a splittable position
        long start = indexFile.getFilePointer();
        builder.addPotentialBoundary(start);

        // encode each value using the appropriate AbstractType
        types.get(0).compress(VERSION, tuples.get(0), indexFile);
        // only recording metadata per key (FIXME: not within supercolumns)
        LongType.encode(new LongList(markedForDeleteAt), indexFile);
        LongType.encode(new LongList(localDeletionTime), indexFile);
        for (int i = 1; i < types.size(); i++)
        {
            LongType.encode(parents.get(i - 1).asLongCollection(), indexFile);
            types.get(i).compress(VERSION, tuples.get(i), indexFile);
        }

        // encode the offsets using the LongType primitive
        LongType.encode(new LongType.LongCollection(offsets.size())
        {
            public long get(int i)
            {
                return offsets.get(i);
            }
        }, indexFile);

        logger.debug("Wrote block of {} positions in {} bytes", offsets.size(), indexFile.getFilePointer() - start);
        clear();
    }

    private void clear()
    {
        for (int i = 0; i < tuples.size(); i++)
            tuples.get(i).clear();
        for (int i = 0; i < parents.size(); i++)
            parents.get(i).clear();
        offsets.clear();
    }
}
