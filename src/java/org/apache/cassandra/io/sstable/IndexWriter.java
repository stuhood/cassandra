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

/**
 * Encapsulates writing the index and filter for an SSTable. The state of this
 * object is not valid until it has been closed.
 */
class IndexWriter implements Closeable
{
    private static Logger logger = LoggerFactory.getLogger(IndexWriter.class);

    private final BufferedRandomAccessFile indexFile;
    public final Descriptor desc;
    public final IPartitioner partitioner;
    public final SegmentedFile.Builder builder;
    public final IndexSummary summary;
    public final BloomFilter bf;
    private FileMark mark;

    IndexWriter(Descriptor desc, IPartitioner part, long keyCount) throws IOException
    {
        this.desc = desc;
        this.partitioner = part;
        indexFile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_INDEX)), "rw", 8 * 1024 * 1024, true);
        builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        summary = new IndexSummary(keyCount);
        bf = BloomFilter.getFilter(keyCount, 15);
    }

    public void afterAppend(DecoratedKey key, long dataPosition) throws IOException
    {
        bf.add(key.key);
        long indexPosition = indexFile.getFilePointer();
        ByteBufferUtil.writeWithShortLength(key.key, indexFile);
        indexFile.writeLong(dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote index of " + key + " at " + indexPosition);

        summary.maybeAddEntry(key, indexPosition);
        builder.addPotentialBoundary(indexPosition);
    }

    /**
        * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
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

        // finalize in-memory index state
        summary.complete();
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
