/*
 * 
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
 * 
 */

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;

/**
 * Tracks the state and thresholds necessary to Chunk an iterator nested at depth N
 * into a Span of N levels/chunks.
 */
class ChunkAppender
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private final BufferedRandomAccessFile dataFile;
    private final List<AbstractType> types;
    // the current chunk for each level
    private final Chunk[] chunks;
    // the currently open range type, and the metadata timestamps to go with it
    private final byte[] openType;
    private final long[] openClient;
    private final int[] openLocal;

    // current and target size of a span, in bytes
    private long spanBytes;
    private final long targetSpanBytes;

    public ChunkAppender(Descriptor desc, List<AbstractType> types, BufferedRandomAccessFile dataFile, int targetSpanSizeInKB)
    {
        this.targetSpanBytes = targetSpanSizeInKB * 1024;
        this.dataFile = dataFile;
        this.types = types;
        this.chunks = new Chunk[types.size()];
        this.openType = new byte[types.size()];
        this.openClient = new long[types.size()];
        this.openLocal = new int[types.size()];
        for (int i = 0; i < types.size(); i++)
            chunks[i] = new Chunk(desc, types.get(i));
    }

    public int append(DecoratedKey decoratedKey, ColumnFamily cf, Iterator<IColumn> iter) throws IOException
    {
        /** key chunk */
        // open a metadata range for the key's "parent": the parent of a key is the
        // column family: if we wanted to support range deletes for keys, they would
        // be stored at this level
        openRangeMetadata(0, null, Long.MIN_VALUE, Integer.MIN_VALUE);
        add(0, decoratedKey.key, Chunk.ENTRY_PARENT);
        /** child chunks */
        int columnCount = addTo(1, cf, iter);
        // close metadata at the key level
        closeRangeMetadata(0, null);

        // force append to finish the row
        // TODO: removing this force would mean we would start storing more than one row per span
        appendAndReset();
        return columnCount;
    }

    /** Recursively encode IColumn instances, flushing them to disk when thresholds are reached. */
    private int addTo(int depth, IColumnContainer container, Iterator<IColumn> iter) throws IOException
    {
        int columnCount = 0;
        // open the metadata range at this level
        openRangeMetadata(depth, null, container);
        if (!iter.hasNext())
        {
            // this parent contains only metadata: child levels need a NULL placeholder
            for (int i = depth + 1; i < chunks.length; i++)
                add(i, null, Chunk.ENTRY_NULL);
        }
        while (iter.hasNext())
        {
            // before appending new columns, see if thresholds are violated
            maybeAppendAndReset();

            IColumn icol = iter.next();
            // supercolumn value: recurse
            if (icol.getClass() == SuperColumn.class)
            {
                add(depth, icol.name(), Chunk.ENTRY_PARENT);
                // recurse
                SuperColumn scol = (SuperColumn)icol;
                addTo(depth + 1, scol, scol.getSortedColumns().iterator());
                continue;
            }
            // standard value
            add(depth, icol.name(), Chunk.ENTRY_NAME);
            byte cm;
            chunks[depth + 1].clientTimestampAdd(icol.timestamp());
            ByteBuffer value;
            if (icol.getClass() == Column.class)
            {
                cm = Chunk.ENTRY_STANDARD;
                value = icol.value();
            }
            else if (icol.getClass() == DeletedColumn.class)
            {
                cm = Chunk.ENTRY_DELETED;
                chunks[depth + 1].localTimestampAdd(icol.getLocalDeletionTime());
                value = null;
            }
            else if (icol.getClass() == ExpiringColumn.class)
            {
                cm = Chunk.ENTRY_EXPIRING;
                // we don't record the TTL for expiring columns: see CASSANDRA-2620
                chunks[depth + 1].localTimestampAdd(icol.getLocalDeletionTime());
                value = icol.value();
            }
            else if (icol.getClass() == CounterColumn.class)
            {
                cm = Chunk.ENTRY_COUNTER;
                CounterColumn col = (CounterColumn)icol;
                chunks[depth + 1].clientTimestampAdd(col.timestampOfLastDelete());
                value = icol.value();
            }
            else
                throw new RuntimeException("FIXME: " + icol.getClass() + " not implemented");
            add(depth + 1, value, cm);
            columnCount++;
        }
        // the end of this parent
        closeRangeMetadata(depth, null);
        return columnCount;
    }

    private void add(int depth, ByteBuffer value, byte cfm)
    {
        if (value != null)
        {
            chunks[depth].values().add(value);
            spanBytes += value.remaining();
        }
        addMeta(depth, cfm);
        // estimate of metadata size
        spanBytes += 3;
    }

    /** Adds range metadata to a chunk. */
    private void openRangeMetadata(int depth, ByteBuffer start, IColumnContainer container)
    {
        openRangeMetadata(depth, start, container.getMarkedForDeleteAt(), container.getLocalDeletionTime());
    }

    private void openRangeMetadata(int depth, ByteBuffer start, long timestamp, int localDeletionTime)
    {
        assert openType[depth] == Chunk.ENTRY_NULL :
            "Range already open in " + chunks[depth] + ": " + openType[depth];
        byte cfm;
        if (start == null)
            cfm = Chunk.ENTRY_RANGE_BEGIN_NULL;
        else
        {
            cfm = Chunk.ENTRY_RANGE_BEGIN;
            chunks[depth].values().add(start);
            spanBytes += start.remaining();
        }
        chunks[depth].clientTimestampAdd(timestamp);
        chunks[depth].localTimestampAdd(localDeletionTime);
        addMeta(depth, cfm);
        // record the open range
        openType[depth] = cfm;
        openClient[depth] = timestamp;
        openLocal[depth] = localDeletionTime;
        // estimate of metadata size
        spanBytes += 3;
    }

    private void closeRangeMetadata(int depth, ByteBuffer end)
    {
        assert openType[depth] != Chunk.ENTRY_NULL :
            "Range not open in " + chunks[depth];
        byte cfm;
        if (end == null)
            cfm = Chunk.ENTRY_RANGE_END_NULL;
        else
        {
            cfm = Chunk.ENTRY_RANGE_END;
            chunks[depth].values().add(end);
            spanBytes += end.remaining();
        }
        addMeta(depth, cfm);
        openType[depth] = Chunk.ENTRY_NULL;
    }

    private void addMeta(int depth, byte cfm)
    {
        chunks[depth].metadataAdd(cfm);
    }

    /**
     * If thresholds have been reached, flush the current span and reset chunks for
     * the next span.
     */
    private void maybeAppendAndReset() throws IOException
    {
        // the threshold of the last level is the most important, since it is the base of the tree
        if (spanBytes >= targetSpanBytes)
            appendAndReset();
    }

    private void appendAndReset() throws IOException
    {
        if (isFlushed())
            return;

        /**
         * Stash state for any open ranges.
         */
        Stash[] stashed = maybeStashRanges();
        /**
         * Flush and clear chunks from top to bottom
         */
        for (int i = 0; i < chunks.length; i++)
        {
            Chunk chunk = chunks[i];
            chunk.append(dataFile);
            chunk.clear();
        }
        spanBytes = 0;
        /**
         * Restore any open ranges.
         */
        maybeRestoreRanges(stashed);
    }

    /**
     * We need to preserve any open ranges but we'd like to avoid
     * allocating new chunks after each flush, so we use temporary variables.
     * TODO: to avoid doing this to too many spans (causing fragmentation), we should
     * eagerly close spans when we finish a key within ~75% of the threshold
     */
    private Stash[] maybeStashRanges()
    {
        Stash[] stashed = null;
        for (int i = 0; i < chunks.length; i++)
        {
            if (openType[i] == Chunk.ENTRY_NULL)
                // if a parent is not open, a child is not open
                break;
            Chunk chunk = chunks[i];

            // lazy init
            if (stashed == null) stashed = new Stash[chunks.length];
            // split the range on the last appended value, and save the open metadata
            stashed[i] = new Stash(chunk.values().get(chunk.values().size() - 1),
                                   openClient[i],
                                   openLocal[i]);
            closeRangeMetadata(i, stashed[i].val);
        }
        return stashed;
    }

    private void maybeRestoreRanges(Stash[] stashed)
    {
        if (stashed == null)
            return;
        for (int i = 0; i < stashed.length; i++)
        {
            Stash s = stashed[i];
            if (s == null)
                // if a parent is not open, a child is not open
                break;
            // restore the range and the current value
            openRangeMetadata(i, s.val, s.client, s.local);
        }
    }

    /** @return True if internal buffers are empty. */
    public boolean isFlushed()
    {
        if (chunks[0].metadata().position() == 0)
            return true;
        return false;
    }

    /** Must be flushed before the underlying DataFile is closed. */
    public void flush() throws IOException
    {
        appendAndReset();
    }

    /** One level of the state of a range that needed to be split across spans. */
    private static final class Stash
    {
        public final ByteBuffer val;
        public final long client;
        public final int local;

        public Stash(ByteBuffer val, long client, int local)
        {
            this.val = val;
            this.client = client;
            this.local = local;
        }
    }
}
