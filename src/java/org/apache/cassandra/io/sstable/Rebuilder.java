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
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.ByteBufferUtil;

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

        return indexer.index();
    }

    class RowIndexer
    {
        public final BufferedRandomAccessFile dfile;

        protected RowIndexer() throws IOException
        {
            this.dfile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "r", 8 * 1024 * 1024, true);
        }

        SSTableReader index() throws IOException
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
        }

        /**
        * If the key is cached, we should:
        *   - For AES: run the newly received row by the cache
        *   - For other: invalidate the cache (even if very unlikely, a key could be in cache in theory if a neighbor was boostrapped and
        *     then removed quickly afterward (a key that we had lost but become responsible again could have stayed in cache). That key
        *     would be obsolete and so we must invalidate the cache).
        */
        protected void updateCache(SSTableIdentityIterator row, long startPosition) throws IOException
        {
            ColumnFamily cached = cfs.getRawCachedRow(row.getKey());
            if (cached == null)
                return;
            switch (type)
            {
                case AES:
                    // slurp the row data into a cf: if we pass the in memory compaction
                    // limit, stop slurping and invalidate
                    ColumnFamily cf = row.getColumnFamily().cloneMeShallow();
                    long limit = startPosition + DatabaseDescriptor.getInMemoryCompactionLimit();
                    while (row.hasNext())
                    {
                        if (dfile.getFilePointer() > limit)
                        {
                            // We have a key in cache for a very big row, that is fishy. We don't fail here however because that would prevent the sstable
                            // from being build (and there is no real point anyway), so we just invalidate the row for correction and log a warning.
                            logger.warn("Found a cached row over the in memory compaction limit during post-streaming rebuilt; it is highly recommended to avoid huge row on column family with row cache enabled.");
                            cf = null;
                            break;
                        }
                        cf.addColumn(row.next());
                    }
                    row.reset();
                    if (cf == null)
                        cfs.invalidateCachedRow(row.getKey());
                    else
                        cfs.updateRowCache(row.getKey(), cf);
                    break;
                default:
                    cfs.invalidateCachedRow(row.getKey());
                    break;
            }
        }

        protected SSTableReader doIndexing() throws IOException
        {
            long estimatedRows = SSTable.estimateRowsFromData(cfs.metadata, cfs.partitioner, desc, dfile);
            IndexWriter iwriter = new IndexWriter(desc, cfs.partitioner, ReplayPosition.NONE, estimatedRows);
            try
            {
                long rows = 0;
                while (!dfile.isEOF())
                {
                    long rowPosition = dfile.getFilePointer();
                    SSTableIdentityIterator iter = SSTableIdentityIterator.create(cfs.metadata,
                                                                                  cfs.partitioner,
                                                                                  desc,
                                                                                  dfile,
                                                                                  true);
                    updateCache(iter, rowPosition);
                    // close the iterator to position ourself at the end of the row
                    // (which will make getDataSize and getColumnCount valid)
                    iter.close();
                    iwriter.afterAppend(iter.getKey(), rowPosition, iter.getDataSize(), iter.getColumnCount());

                    rows++;
                }
                logger.debug("estimated row count was {} of real count", ((double)estimatedRows) / rows);
            }
            finally
            {
                // we can close quietly because the open will fail if this was not successful
                FileUtils.closeQuietly(iwriter);
            }

            return SSTableReader.open(SSTableWriter.rename(desc, SSTable.componentsFor(desc, false)));
        }
    }

    /*
     * When a sstable for a counter column family is streamed, we must ensure
     * that on the receiving node all counter column goes through the
     * deserialization from remote code path (i.e, it must be cleared from its
     * delta) to maintain the invariant that on a given node, only increments
     * that the node originated are delta (and copy of those must not be delta).  *
     * Since after streaming row indexation goes through every streamed
     * sstable, we use this opportunity to ensure this property. This is the
     * goal of this specific CommutativeRowIndexer.
     */
    class CommutativeRowIndexer extends RowIndexer
    {
        protected BufferedRandomAccessFile writerDfile;

        public CommutativeRowIndexer() throws IOException
        {
            super();
        }

        @Override
        protected SSTableReader doIndexing() throws IOException
        {
            // read from the input and write to a new output sstable, clearing as we go
            long estimatedRows = SSTable.estimateRowsFromData(cfs.metadata, cfs.partitioner, desc, dfile);
            SSTableWriter writer = cfs.createFlushWriter(estimatedRows, dfile.length(), ReplayPosition.NONE);
            try
            {
                CompactionController controller = new CompactionController(cfs, Collections.<SSTableReader>emptyList(), Integer.MAX_VALUE, true);
                long dfileLength = dfile.length();
                while (!dfile.isEOF())
                {
                    long rowPosition = dfile.getFilePointer();
                    SSTableIdentityIterator iter = SSTableIdentityIterator.create(cfs.metadata,
                                                                                  cfs.partitioner,
                                                                                  desc,
                                                                                  dfile,
                                                                                  true);
                    updateCache(iter, rowPosition);
                    CompactedRow row = controller.getCompactedRow(iter);
                    // append and close
                    writer.append(row);
                    iter.close();
                }
            }
            catch (IOException e)
            {
                // clean up the output; trust the caller to cleanup the input
                FileUtils.closeQuietly(writer);
                SSTable.delete(writer.descriptor, SSTable.componentsFor(writer.descriptor, false));
                throw e;
            }
            return writer.closeAndOpenReader();
        }
    }
}
