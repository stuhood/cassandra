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
 * Writes the legacy sstable index, which contained only keys and long offsets.
 * Implements shouldAppend and appendToIndex such that only key entries are added.
 */
class BasicIndexWriter extends IndexWriter
{
    protected BasicIndexWriter(Descriptor desc, IPartitioner part, long keyCount) throws IOException
    {
        super(desc, part, keyCount);
    }

    /** @return An Observer to collect only the keys. */
    @Override
    public Observer observer()
    {
        return new Observer(1, 1, DatabaseDescriptor.getColumnIndexSize(), Long.MAX_VALUE)
        {
            /** Collect the first entry at depth 0. */
            public boolean shouldAdd(int depth, boolean last)
            {
                return depth == 0 && !last;
            }
        };
    }

    @Override
    protected void appendToIndex(Observer observer, long dataSize) throws IOException
    {
        DecoratedKey key = observer.keys.get(0);
        long dataPosition = observer.offsets.get(0);
        long indexPosition = indexFile.getFilePointer();
        ByteBufferUtil.writeWithShortLength(key.key, indexFile);
        // write the position of the data to the index
        indexFile.writeLong(dataPosition);
        long indexDataSize = indexFile.getFilePointer() - indexPosition;
        if (logger.isTraceEnabled())
            logger.trace("wrote index of " + key + " at " + indexPosition);
        summary.maybeAddEntry(key, indexPosition);
        builder.addPotentialBoundary(indexPosition);
    }
}
