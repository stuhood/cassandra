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
import java.util.*;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class SSTableWriter extends SSTable
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    protected SSTableMetadata.Collector sstableMetadataCollector;

    protected static Set<Component> components(CFMetaData metadata)
    {
        Set<Component> components = new HashSet<Component>(Arrays.asList(Component.DATA, Component.FILTER, Component.PRIMARY_INDEX, Component.STATS));
        if (metadata.useCompression())
            components.add(Component.COMPRESSION_INFO);
        return components;
    }

    protected SSTableWriter(Descriptor desc,
                            long keyCount,
                            CFMetaData cfm,
                            IPartitioner partitioner,
                            SSTableMetadata.Collector sstableMetadataCollector) throws IOException
    {
        super(desc,
              components(cfm),
              cfm,
              partitioner);
        this.sstableMetadataCollector = sstableMetadataCollector;
    }
    
    public static SSTableWriter create(String filename, long keyCount) throws IOException
    {
        return create(filename,
                      keyCount,
                      Schema.instance.getCFMetaData(Descriptor.fromFilename(filename)),
                      StorageService.getPartitioner(),
                      SSTableMetadata.createCollector());
    }

    public static SSTableWriter create(String filename,
                                       long keyCount,
                                       CFMetaData metadata,
                                       IPartitioner partitioner,
                                       SSTableMetadata.Collector sstableMetadataCollector) throws IOException
    {
        Descriptor desc = Descriptor.fromFilename(filename);
        return desc.version.isRowIndexed ?
            new RowIndexedWriter(desc,
                                 keyCount,
                                 metadata,
                                 partitioner,
                                 sstableMetadataCollector) :
            new ChunkedWriter(desc,
                              keyCount,
                              metadata,
                              partitioner,
                              sstableMetadataCollector);
    }

    public abstract void mark() throws IOException;

    public abstract void resetAndTruncate() throws IOError;

    /** Appends a compacted row and optionally computes a header/cache entry. */
    public abstract BlockHeader append(AbstractCompactedRow row, boolean computeHeader) throws IOException;

    public abstract void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException;

    /** @Deprecated */
    public abstract void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException;

    public void updateMaxTimestamp(long timestamp)
    {
        sstableMetadataCollector.updateMaxTimestamp(timestamp);
    }

    /**
     * Attempt to close the index writer and data file before deleting all temp components for the sstable
     */
    public abstract void cleanupIfNecessary();

    public SSTableReader closeAndOpenReader() throws IOException
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public abstract SSTableReader closeAndOpenReader(long maxDataAge) throws IOException;

    protected static void writeMetadata(Descriptor desc, SSTableMetadata sstableMetadata) throws IOException
    {
        SequentialWriter out = SequentialWriter.open(new File(desc.filenameFor(SSTable.COMPONENT_STATS)), true);
        SSTableMetadata.serializer.serialize(sstableMetadata, out.stream);
        out.close();
    }

    static Descriptor rename(Descriptor tmpdesc, Set<Component> components)
    {
        Descriptor newdesc = tmpdesc.asTemporary(false);
        try
        {
            // do -Data last because -Data present should mean the sstable was completely renamed before crash
            for (Component component : Sets.difference(components, Collections.singleton(Component.DATA)))
                FBUtilities.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
            FBUtilities.renameWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return newdesc;
    }

    public abstract long getFilePointer();
}
