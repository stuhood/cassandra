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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * Encapsulates writing the index, filter and statistics for an SSTable. The state of this
 * object is not valid until it has been closed.
 *
 * For every tuple in a file, a consumer should call (with the depth of the tuple as a parameter):
 * shouldAppend, [append], increment
 */
abstract class IndexWriter
{
    protected static Logger logger = LoggerFactory.getLogger(IndexWriter.class);

    protected final BufferedRandomAccessFile indexFile;
    public final Descriptor desc;
    public final IPartitioner partitioner;
    public final SegmentedFile.Builder builder;
    public final IndexSummary summary;
    public final BloomFilter bf;
    public final EstimatedHistogram estimatedRowSize;
    public final EstimatedHistogram estimatedColumnCount;

    private FileMark mark;
    /**
     * TODO: both IndexSummary and IndexWriter track the number of items, because
     * IndexSummary is used independently when loading an index from disk: should
     * find a way to split the tracking out.
     */
    private long[] written;

    protected IndexWriter(Descriptor desc, IPartitioner part, long keyCount) throws IOException
    {
        this.desc = desc;
        this.partitioner = part;
        indexFile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_INDEX)), "rw", 8 * 1024 * 1024, true);
        builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        summary = new IndexSummary(keyCount);
        bf = BloomFilter.getFilter(keyCount, 15);
        estimatedRowSize = SSTable.defaultRowHistogram();
        estimatedColumnCount = SSTable.defaultColumnHistogram();
    }

    static IndexWriter create(Descriptor desc, CFMetaData meta, IPartitioner part, long keyCount) throws IOException
    {
        if (desc.hasBasicIndex)
            return new BasicIndexWriter(desc, part, keyCount);
        return new NestedIndexWriter(desc, meta, part, keyCount);
    }

    /** @return An Observer to collect values from the data file. */
    public abstract Observer observer();

    /**
     * Appends the content of an Observer for a row and resets it for reuse.
     * TODO: Once we are using a block based data file format, the unit of observation
     * should be a block.
     */
    public void append(Observer observer, long dataSize) throws IOException
    {
        // assuming a single key/row per observer
        DecoratedKey key = observer.keys.get(0);
        bf.add(key.key);
        estimatedRowSize.add(dataSize);
        // the observer has been incremented for every entry
        estimatedColumnCount.add(observer.count());
        appendToIndex(observer, dataSize);
        observer.reset(true);
    }

    /**
     * Appends an observer containing content for a row to the index file.
     */
    protected abstract void appendToIndex(Observer observer, long dataSize) throws IOException;

    /**
     * Flushes the state of the index writer.
     */
    protected void flush() throws IOException {}

    /**
     * Closes all components, making the public state of this writer valid for consumption.
     */
    public void close() throws IOException
    {
        flush();
        
        // bloom filter
        FileOutputStream fos = new FileOutputStream(desc.filenameFor(SSTable.COMPONENT_FILTER));
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(bf, stream);
        stream.flush();
        fos.getFD().sync();
        stream.close();

        // index
        long position = indexFile.getFilePointer();
        indexFile.close(); // calls force
        FileUtils.truncate(indexFile.getPath(), position);
        summary.complete();

        // statistics
        BufferedRandomAccessFile out = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_STATS)),
                                                                    "rw",
                                                                    BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE,
                                                                    true);
        EstimatedHistogram.serializer.serialize(estimatedRowSize, out);
        EstimatedHistogram.serializer.serialize(estimatedColumnCount, out);
        out.close();
    }

    public void mark()
    {
        // nothing to do: a row is added in one go using append(Observer)
    }

    public void reset() throws IOException
    {
        // nothing to do: a row is added in one go using append(Observer)
    }
}
