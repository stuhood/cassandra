/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.streaming;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.PrecompactedRow;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BytesReadTracker;
import org.apache.cassandra.utils.Pair;

public class IncomingStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingStreamReader.class);

    protected final PendingFile localFile;
    protected final PendingFile remoteFile;
    protected final StreamInSession session;
    private final Socket socket;

    public IncomingStreamReader(StreamHeader header, Socket socket) throws IOException
    {
        this.socket = socket;
        InetSocketAddress remoteAddress = (InetSocketAddress)socket.getRemoteSocketAddress();
        session = StreamInSession.get(remoteAddress.getAddress(), header.sessionId);
        session.addFiles(header.pendingFiles);
        // set the current file we are streaming so progress shows up in jmx
        session.setCurrentFile(header.file);
        session.setTable(header.table);
        // pendingFile gets the new context for the local node.
        remoteFile = header.file;
        localFile = remoteFile != null ? StreamIn.getContextMapping(remoteFile) : null;
    }

    public void read() throws IOException
    {
        if (remoteFile != null)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Receiving stream");
                logger.debug("Creating file for {} with {} estimated keys",
                             localFile.getFilename(),
                             remoteFile.estimatedKeys);
            }

            assert remoteFile.estimatedKeys > 0;
            SSTableReader reader = null;
            logger.debug("Estimated keys {}", remoteFile.estimatedKeys);
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            try
            {
                reader = streamIn(dis, localFile, remoteFile);
            }
            catch (IOException ex)
            {
                retry();
                throw ex;
            }
            finally
            {
                dis.close();
            }

            session.finished(remoteFile, reader);
        }

        session.closeIfFinished();
    }

    private SSTableReader streamIn(DataInput input, PendingFile localFile, PendingFile remoteFile) throws IOException
    {
        ColumnFamilyStore cfs = Table.open(localFile.desc.ksname).getColumnFamilyStore(localFile.desc.cfname);
        SSTableWriter writer = SSTableWriter.create(localFile.getFilename(), remoteFile.estimatedKeys);
        CompactionController controller = new CompactionController(cfs, Collections.<SSTableReader>emptyList(), Integer.MAX_VALUE, true);
        Cursor cursor = new Cursor(localFile.desc, cfs.metadata.getTypes());

        try
        {
            BytesReadTracker in = new BytesReadTracker(input);

            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            System.out.println("on-the-fly-rebuild for " + localFile + "|" + localFile.sections);
            for (Pair<Long, Long> section : localFile.sections)
            {
                long length = section.right - section.left;
                long bytesRead = 0;
                while (bytesRead < length || cursor.isAvailable())
                {
                    in.reset(0);

                    SSTableIdentityIterator iter = SSTableIdentityIterator.create(cfs.metadata, StorageService.getPartitioner(), localFile.desc, in, cursor, true);
                    AbstractCompactedRow row = controller.getCompactedRow(iter);
                    writer.append(row, false);
                    // row append does not update the max timestamp on its own
                    writer.updateMaxTimestamp(row.maxTimestamp());

                    // update cache
                    ColumnFamily cached = cfs.getRawCachedRow(iter.getKey());
                    if (cached != null)
                    {
                        switch (remoteFile.type)
                        {
                            case AES:
                                ColumnFamily cf = row.getFullColumnFamily();
                                if (cf != null)
                                    // row was narrow enough to fit in memory
                                    cfs.updateRowCache(iter.getKey(), cf);
                                else
                                {
                                    // We have a key in cache for a very big row, that is fishy. We don't fail here however because that would prevent the sstable
                                    // from being build (and there is no real point anyway), so we just invalidate the row for correction and log a warning.
                                    logger.warn("Found a cached row over the in memory compaction limit during post-streaming rebuilt; it is highly recommended to avoid huge row on column family with row cache enabled.");
                                    cfs.invalidateCachedRow(iter.getKey());
                                }
                                break;
                            default:
                                break;
                        }
                    }

                    // close the row to ensure it is consumed
                    iter.close();
                    bytesRead += in.getBytesRead();
                    remoteFile.progress += in.getBytesRead();
                }
            }
            System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            return writer.closeAndOpenReader();
        }
        finally
        {
            writer.cleanupIfNecessary();
        }
    }

    private void retry() throws IOException
    {
        /* Ask the source node to re-stream this file. */
        session.retry(remoteFile);

        /* Delete the orphaned file. */
        FileUtils.deleteWithConfirm(new File(localFile.getFilename()));
    }
}
