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

public class RowIndexedWriter extends SSTableWriter
{
    private static Logger logger = LoggerFactory.getLogger(RowIndexedWriter.class);

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;

    RowIndexedWriter(Descriptor desc,
                     long keyCount,
                     CFMetaData metadata,
                     IPartitioner partitioner,
                     SSTableMetadata.Collector sstableMetadataCollector) throws IOException
    {
        super(desc,
              keyCount,
              metadata,
              partitioner,
              sstableMetadataCollector);
        iwriter = new IndexWriter(descriptor, partitioner, keyCount);

        if (compression)
        {
            dbuilder = SegmentedFile.getCompressedBuilder();
            dataFile = CompressedSequentialWriter.open(getFilename(),
                                                       descriptor.filenameFor(Component.COMPRESSION_INFO),
                                                       true);
        }
        else
        {
            dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
            dataFile = SequentialWriter.open(new File(getFilename()), true);
        }
    }
    
    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void resetAndTruncate()
    {
        try
        {
            dataFile.resetAndTruncate(dataMark);
            iwriter.resetAndTruncate();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private long beforeAppend(DecoratedKey decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + getFilename());
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataPosition) throws IOException
    {
        lastWrittenKey = decoratedKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        iwriter.afterAppend(decoratedKey, dataPosition);
        dbuilder.addPotentialBoundary(dataPosition);
    }

    public long append(AbstractCompactedRow row) throws IOException
    {
        long currentPosition = beforeAppend(row.key);
        ByteBufferUtil.writeWithShortLength(row.key.key, dataFile.stream);
        long dataStart = dataFile.getFilePointer();
        long dataSize = row.write(dataFile.stream);
        assert dataSize == dataFile.getFilePointer() - (dataStart + 8)
                : "incorrect row data size " + dataSize + " written to " + dataFile.getPath() + "; correct is " + (dataFile.getFilePointer() - (dataStart + 8));
        // max timestamp is not collected here, because we want to avoid deserializing an EchoedRow
        // instead, it is collected when calling ColumnFamilyStore.createCompactionWriter
        sstableMetadataCollector.addRowSize(dataFile.getFilePointer() - currentPosition);
        sstableMetadataCollector.addColumnCount(row.columnCount());
        afterAppend(row.key, currentPosition);
        return currentPosition;
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile.stream);

        // serialize index and bloom filter into in-memory structure
        ColumnIndexer.RowHeader header = ColumnIndexer.serialize(cf);

        // write out row size
        dataFile.stream.writeLong(header.serializedSize() + cf.serializedSizeForSSTable());

        // write out row header and data
        int columnCount = ColumnFamily.serializer().serializeWithIndexes(cf, header, dataFile.stream);
        afterAppend(decoratedKey, startPosition);

        // track max column timestamp
        sstableMetadataCollector.updateMaxTimestamp(cf.maxTimestamp());
        sstableMetadataCollector.addRowSize(dataFile.getFilePointer() - startPosition);
        sstableMetadataCollector.addColumnCount(columnCount);
    }

    public void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile.stream);
        assert value.remaining() > 0;
        dataFile.stream.writeLong(value.remaining());
        ByteBufferUtil.write(value, dataFile.stream);
        afterAppend(decoratedKey, currentPosition);
    }

    /**
     * Attempt to close the index writer and data file before deleting all temp components for the sstable
     */
    public void cleanupIfNecessary()
    {
        FileUtils.closeQuietly(iwriter);
        FileUtils.closeQuietly(dataFile);

        try
        {
            Set<Component> components = SSTable.componentsFor(descriptor, Descriptor.TempState.TEMP);
            if (!components.isEmpty())
                SSTable.delete(descriptor, components);
        }
        catch (Exception e)
        {
            logger.error(String.format("Failed deleting temp components for %s", descriptor), e);
        }
    }

    public SSTableReader closeAndOpenReader() throws IOException
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge) throws IOException
    {
        // index and filter
        iwriter.close();

        // main data, close will truncate if necessary
        dataFile.close();

        // write sstable statistics
        SSTableMetadata sstableMetadata = sstableMetadataCollector.finalizeMetadata();
        writeMetadata(descriptor, sstableMetadata);

        // remove the 'tmp' marker from all components
        final Descriptor newdesc = rename(descriptor, components);

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(newdesc.filenameFor(SSTable.COMPONENT_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_DATA));
        SSTableReader sstable = SSTableReader.internalOpen(newdesc,
                                                           components,
                                                           metadata,
                                                           partitioner,
                                                           ifile,
                                                           dfile,
                                                           iwriter.summary,
                                                           iwriter.bf,
                                                           maxDataAge,
                                                           sstableMetadata);
        iwriter = null;
        dbuilder = null;
        return sstable;
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    static class IndexWriter implements Closeable
    {
        private final SequentialWriter indexFile;
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
            indexFile = SequentialWriter.open(new File(desc.filenameFor(SSTable.COMPONENT_INDEX)), 8 * 1024 * 1024, true);
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
            summary = new IndexSummary(keyCount);
            bf = BloomFilter.getFilter(keyCount, 15);
        }

        public void afterAppend(DecoratedKey key, long dataPosition) throws IOException
        {
            bf.add(key.key);
            long indexPosition = indexFile.getFilePointer();
            ByteBufferUtil.writeWithShortLength(key.key, indexFile.stream);
            indexFile.stream.writeLong(dataPosition);
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

        public void resetAndTruncate() throws IOException
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            indexFile.resetAndTruncate(mark);
        }

        public String toString()
        {
            return "IndexWriter(" + desc + ")";
        }
    }
}
