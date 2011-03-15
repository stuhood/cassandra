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
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.AbstractCompactedRow;
import org.apache.cassandra.io.CompactionController;
import org.apache.cassandra.io.ICompactionInfo;
import org.apache.cassandra.io.LazilyCompactedRow;
import org.apache.cassandra.io.PrecompactedRow;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Rebuilds components of an SSTable based on its data file: typically used after
 * streaming, and always executed in the Compaction executor.
 */
public class Rebuilder implements ICompactionInfo
{
    private static Logger logger = LoggerFactory.getLogger(Rebuilder.class);

    private final Descriptor desc;
    private final OperationType type;
    private final ColumnFamilyStore cfs;
    private RowIndexer indexer;

    public Rebuilder(Descriptor desc, OperationType type)
    {
        this.desc = desc;
        this.type = type;
        cfs = Table.open(desc.ksname).getColumnFamilyStore(desc.cfname);
    }

    public static Rebuilder create(Descriptor desc, OperationType type)
    {
        if (!desc.isLatestVersion)
            // TODO: streaming between different versions will fail: need support for
            // recovering other versions to provide a stable streaming api
            throw new RuntimeException(String.format("Cannot recover SSTable with version %s (current version %s).",
                                                     desc.version, Descriptor.CURRENT_VERSION));

        return new Rebuilder(desc, type);
    }

    // lazy-initialize the file to avoid opening it until it's actually executing on the CompactionManager,
    // since the 8MB buffers can use up heap quickly
    private void maybeOpenIndexer()
    {
        if (indexer != null)
            return;
        try
        {
            if (cfs.metadata.getDefaultValidator().isCommutative())
                indexer = new CommutativeRowIndexer(desc, cfs.metadata);
            else
                indexer = new RowIndexer(desc, cfs.metadata);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public SSTableReader build() throws IOException
    {
        if (cfs.isInvalid())
            return null;
        maybeOpenIndexer();

        File ifile = new File(desc.filenameFor(SSTable.COMPONENT_INDEX));
        File ffile = new File(desc.filenameFor(SSTable.COMPONENT_FILTER));
        assert !ifile.exists();
        assert !ffile.exists();

        long estimatedRows = indexer.prepareIndexing();

        // build the index and filter
        long rows = indexer.index();

        logger.debug("estimated row count was {} of real count", ((double)estimatedRows) / rows);
        return SSTableReader.open(SSTableWriter.rename(desc, SSTable.componentsFor(desc)));
    }

    public long getTotalBytes()
    {
        maybeOpenIndexer();
        try
        {
            // (length is still valid post-close)
            return indexer.dfile.length();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public long getBytesComplete()
    {
        maybeOpenIndexer();
        // (getFilePointer is still valid post-close)
        return indexer.dfile.getFilePointer();
    }

    public String getTaskType()
    {
        return "SSTable rebuild";
    }

    static class RowIndexer
    {
        protected final Descriptor desc;
        public final BufferedRandomAccessFile dfile;

        protected IndexWriter iwriter;
        protected CFMetaData metadata;

        RowIndexer(Descriptor desc, CFMetaData metadata) throws IOException
        {
            this(desc, new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "r", 8 * 1024 * 1024, true), metadata);
        }

        protected RowIndexer(Descriptor desc, BufferedRandomAccessFile dfile, CFMetaData metadata) throws IOException
        {
            this.desc = desc;
            this.dfile = dfile;
            this.metadata = metadata;
        }

        long prepareIndexing() throws IOException
        {
            long estimatedRows;
            try
            {
                estimatedRows = SSTable.estimateRowsFromData(desc, dfile);
                iwriter = new IndexWriter(desc, StorageService.getPartitioner(), estimatedRows);
                return estimatedRows;
            }
            catch(IOException e)
            {
                dfile.close();
                throw e;
            }
        }

        long index() throws IOException
        {
            try
            {
                return doIndexing();
            }
            finally
            {
                try
                {
                    close();
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        }

        void close() throws IOException
        {
            dfile.close();
            iwriter.close();
        }

        protected long doIndexing() throws IOException
        {
            long rows = 0;
            DecoratedKey key;
            long rowPosition = 0;
            long nextRowPosition = 0;
            while (rowPosition < dfile.length())
            {
                // read key
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, ByteBufferUtil.readWithShortLength(dfile));

                // determine where the row ends
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                nextRowPosition = dfile.getFilePointer() + dataSize;
                
                IndexHelper.skipBloomFilter(dfile);
                IndexHelper.skipIndex(dfile);
                ColumnFamily.serializer().deserializeFromSSTableNoColumns(ColumnFamily.create(metadata), dfile);
                long columnCount = dfile.readInt();

                // append row info to the index and move to the next row
                iwriter.afterAppend(key, rowPosition, dataSize, columnCount);
                rowPosition = nextRowPosition;
                dfile.seek(rowPosition);
                rows++;
            }
            return rows;
        }
    }

    /*
     * When a sstable for a counter column family is streamed, we must ensure
     * that on the receiving node all counter column goes through the
     * deserialization from remote code path (i.e, it must be cleared from its
     * delta) to maintain the invariant that on a given node, only increments
     * that the node originated are delta (and copy of those must not be delta).
     *
     * Since after streaming row indexation goes through every streamed
     * sstable, we use this opportunity to ensure this property. This is the
     * goal of this specific CommutativeRowIndexer.
     */
    static class CommutativeRowIndexer extends RowIndexer
    {
        protected BufferedRandomAccessFile writerDfile;

        CommutativeRowIndexer(Descriptor desc, CFMetaData metadata) throws IOException
        {
            super(desc, new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "r", 8 * 1024 * 1024, true), metadata);
            writerDfile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "rw", 8 * 1024 * 1024, true);
        }

        @Override
        protected long doIndexing() throws IOException
        {
            long rows = 0L;
            DecoratedKey key;

            CompactionController controller = CompactionController.getBasicController(true);

            long dfileLength = dfile.length();
            while (!dfile.isEOF())
            {
                // read key
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, ByteBufferUtil.readWithShortLength(dfile));

                // skip data size, bloom filter, column index
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                SSTableIdentityIterator iter = new SSTableIdentityIterator(metadata, dfile, key, dfile.getFilePointer(), dataSize, true);

                AbstractCompactedRow row;
                if (dataSize > DatabaseDescriptor.getInMemoryCompactionLimit())
                {
                    logger.info(String.format("Rebuilding post-streaming large counter row %s (%d bytes) incrementally", ByteBufferUtil.bytesToHex(key.key), dataSize));
                    row = new LazilyCompactedRow(controller, Collections.singletonList(iter));
                }
                else
                {
                    row = new PrecompactedRow(controller, Collections.singletonList(iter));
                }

                // update index writer
                iwriter.afterAppend(key, writerDfile.getFilePointer(), dataSize, row.columnCount());
                // write key and row
                ByteBufferUtil.writeWithShortLength(key.key, writerDfile);
                row.write(writerDfile);

                rows++;
            }

            if (writerDfile.getFilePointer() != dfile.getFilePointer())
            {
                // truncate file to new, reduced length
                writerDfile.setLength(writerDfile.getFilePointer());
            }
            writerDfile.sync();

            return rows;
        }


        @Override
        void close() throws IOException
        {
            super.close();
            writerDfile.close();
        }
    }
}
