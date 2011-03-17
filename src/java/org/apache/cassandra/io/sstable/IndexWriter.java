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
import org.apache.cassandra.db.commitlog.ReplayPosition;
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
 */
abstract class IndexWriter
{
    protected static Logger logger = LoggerFactory.getLogger(IndexWriter.class);

    protected final BufferedRandomAccessFile indexFile;
    public final Descriptor desc;
    public final IPartitioner partitioner;
    public final ReplayPosition replayPosition;
    public final SegmentedFile.Builder builder;
    public final IndexSummary summary;
    public final BloomFilter bf;
    public final EstimatedHistogram estimatedRowSize;
    public final EstimatedHistogram estimatedColumnCount;

    private FileMark mark;

    protected IndexWriter(Descriptor desc, IPartitioner part, ReplayPosition rp, long keyCount) throws IOException
    {
        this.desc = desc;
        this.partitioner = part;
        this.replayPosition = rp;
        indexFile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_INDEX)), "rw", 8 * 1024 * 1024, true);
        builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        summary = new IndexSummary(keyCount);
        bf = BloomFilter.getFilter(keyCount, 15);
        estimatedRowSize = SSTable.defaultRowHistogram();
        estimatedColumnCount = SSTable.defaultColumnHistogram();
    }

    static IndexWriter create(Descriptor desc, IPartitioner part, ReplayPosition rp, long keyCount) throws IOException
    {
        return new BasicIndexWriter(desc, part, rp, keyCount);
    }

    public void afterAppend(DecoratedKey key, long dataPosition, long dataSize, long columnCount) throws IOException
    {
        bf.add(key.key);
        estimatedRowSize.add(dataSize);
        estimatedColumnCount.add(columnCount);
        builder.addPotentialBoundary(appendToIndex(key, dataPosition));
    }

    /**
     * Appends the given key to the index file and returns the offset in the index file that
     * it was written at.
     */
    protected abstract long appendToIndex(DecoratedKey key, long dataPosition) throws IOException;

    /**
     * Closes all components, making the public state of this writer valid for consumption.
     */
    public void close() throws IOException
    {
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
        ReplayPosition.serializer.serialize(replayPosition, out);
        out.close();
    }

    public void mark()
    {
        mark = indexFile.mark();
    }

    public void reset() throws IOException
    {
        // we can't un-set the bloom filter addition, but extra keys in there are harmless.
        // we can't reset dbuilder either, but that is the last thing called in afterappend so
        // we assume that if that worked then we won't be trying to reset.
        indexFile.reset(mark);
    }
}
