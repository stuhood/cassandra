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
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.*;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * Rebuilds components of an SSTable based on its data file: typically used after
 * streaming, and always executed in the Compaction executor.
 */
public class Rebuilder implements CompactionInfo.Holder
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

    public CompactionInfo getCompactionInfo()
    {
        maybeOpenIndexer();
        try
        {
            // both file offsets are still valid post-close
            return new CompactionInfo(desc.ksname,
                                      desc.cfname,
                                      CompactionType.SSTABLE_BUILD,
                                      indexer.dfile.getFilePointer(),
                                      indexer.dfile.length());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
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
                indexer = new CommutativeRowIndexer();
            else
                indexer = new RowIndexer();
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
        return SSTableReader.open(SSTableWriter.rename(desc, SSTable.componentsFor(desc, false)));
    }

    class RowIndexer
    {
        public final BufferedRandomAccessFile dfile;
        protected IndexWriter iwriter;

        protected RowIndexer() throws IOException
        {
            this.dfile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "r", 8 * 1024 * 1024, true);
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

        /**
        * If the key is cached, we should:
        *   - For AES: run the newly received row by the cache
        *   - For other: invalidate the cache (even if very unlikely, a key could be in cache in theory if a neighbor was boostrapped and
        *     then removed quickly afterward (a key that we had lost but become responsible again could have stayed in cache). That key
        *     would be obsolete and so we must invalidate the cache).
        */
        protected void updateCache(DecoratedKey key, long dataSize, AbstractCompactedRow row) throws IOException
        {
            ColumnFamily cached = cfs.getRawCachedRow(key);
            if (cached != null)
            {
                switch (type)
                {
                    case AES:
                        if (dataSize > DatabaseDescriptor.getInMemoryCompactionLimit())
                        {
                            // We have a key in cache for a very big row, that is fishy. We don't fail here however because that would prevent the sstable
                            // from being build (and there is no real point anyway), so we just invalidate the row for correction and log a warning.
                            logger.warn("Found a cached row over the in memory compaction limit during post-streaming rebuilt; it is highly recommended to avoid huge row on column family with row cache enabled.");
                            cfs.invalidateCachedRow(key);
                        }
                        else
                        {
                            ColumnFamily cf;
                            if (row == null)
                            {
                                // If not provided, read from disk.
                                cf = ColumnFamily.create(cfs.metadata);
                                ColumnFamily.serializer().deserializeColumns(dfile, cf, true, true);
                            }
                            else
                            {
                                assert row instanceof PrecompactedRow;
                                // we do not purge so we should not get a null here
                                cf = ((PrecompactedRow)row).getFullColumnFamily();
                            }
                            cfs.updateRowCache(key, cf);
                        }
                        break;
                    default:
                        cfs.invalidateCachedRow(key);
                        break;
                }
            }
        }

        protected long doIndexing() throws IOException
        {
            EstimatedHistogram rowSizes = SSTable.defaultRowHistogram();
            EstimatedHistogram columnCounts = SSTable.defaultColumnHistogram();
            long rows = 0;
            DecoratedKey key;
            long rowPosition = 0;
            while (rowPosition < dfile.length())
            {
                // read key
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, ByteBufferUtil.readWithShortLength(dfile));
                iwriter.afterAppend(key, rowPosition);

                // seek to next key
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                rowPosition = dfile.getFilePointer() + dataSize;
                
                IndexHelper.skipBloomFilter(dfile);
                IndexHelper.skipIndex(dfile);
                ColumnFamily.serializer().deserializeFromSSTableNoColumns(ColumnFamily.create(cfs.metadata), dfile);

                // don't move that statement around, it expects the dfile to be before the columns
                updateCache(key, dataSize, null);

                rowSizes.add(dataSize);
                columnCounts.add(dfile.readInt());
                dfile.seek(rowPosition);
                rows++;
            }
            SSTableWriter.writeMetadata(desc, rowSizes, columnCounts, ReplayPosition.NONE);
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
    class CommutativeRowIndexer extends RowIndexer
    {
        protected BufferedRandomAccessFile writerDfile;

        CommutativeRowIndexer() throws IOException
        {
            super();
            writerDfile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "rw", 8 * 1024 * 1024, true);
        }

        @Override
        protected long doIndexing() throws IOException
        {
            EstimatedHistogram rowSizes = SSTable.defaultRowHistogram();
            EstimatedHistogram columnCounts = SSTable.defaultColumnHistogram();
            long rows = 0L;
            DecoratedKey key;

            CompactionController controller = new CompactionController(cfs, Collections.<SSTableReader>emptyList(), Integer.MAX_VALUE, true);

            while (!dfile.isEOF())
            {
                // read key
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, ByteBufferUtil.readWithShortLength(dfile));

                // skip data size, bloom filter, column index
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                SSTableIdentityIterator iter = new SSTableIdentityIterator(cfs.metadata, dfile, key, dfile.getFilePointer(), dataSize, true);

                AbstractCompactedRow row = controller.getCompactedRow(iter);
                updateCache(key, dataSize, row);

                rowSizes.add(dataSize);
                columnCounts.add(row.columnCount());

                // update index writer
                iwriter.afterAppend(key, writerDfile.getFilePointer());
                // write key and row
                ByteBufferUtil.writeWithShortLength(key.key, writerDfile);
                row.write(writerDfile);

                rows++;
            }
            SSTableWriter.writeMetadata(desc, rowSizes, columnCounts, ReplayPosition.NONE);

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
