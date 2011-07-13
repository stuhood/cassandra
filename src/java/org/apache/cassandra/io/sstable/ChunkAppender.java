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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.SequentialWriter;

/**
 * Tracks the state and thresholds necessary to Chunk an iterator nested at depth N
 * into a Span of N levels/chunks.
 */
class ChunkAppender
{
    private static Logger logger = LoggerFactory.getLogger(ChunkedWriter.class);

    private final List<AbstractType> types;
    private final ChunkedWriter.IndexWriter iwriter;
    private final SSTableMetadata.Collector sstmc;
    private final SequentialWriter dataFile;
    // the current chunk for each level
    private final Chunk[] chunks;
    // the currently open range type, and the metadata timestamps to go with it
    private final byte[] openType;
    private final long[] openClient;
    private final int[] openLocal;

    private final long targetBlockSize;
    private long blockSize = 0;

    private long blocks = 0;

    public ChunkAppender(Descriptor desc, CFMetaData meta, SequentialWriter dataFile, ChunkedWriter.IndexWriter iwriter, SSTableMetadata.Collector sstmc)
    {
        this.types = meta.getTypes();
        this.iwriter = iwriter;
        this.sstmc = sstmc;
        this.dataFile = dataFile;
        this.chunks = new Chunk[types.size()];
        this.openType = new byte[types.size()];
        this.openClient = new long[types.size()];
        this.openLocal = new int[types.size()];
        for (int i = 0; i < types.size(); i++)
            chunks[i] = new Chunk(desc, types.get(i));
        targetBlockSize = meta.getBlockSizeInKB() * 1024;

        // open a metadata range for the key's "parent": the parent of a key is the
        // column family: if we wanted to support range deletes for keys, they would
        // be stored at this level
        openRangeMetadata(0, null, Long.MIN_VALUE, Integer.MIN_VALUE);
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf, Iterator<IColumn> iter) throws IOException
    {
        if (blockSize > targetBlockSize)
            appendAndReset();
        /** key chunk */
        addEntry(0, decoratedKey.key, Chunk.ENTRY_PARENT);
        iwriter.add(decoratedKey, dataFile.getFilePointer());
        iwriter.add(cf.getMarkedForDeleteAt(), cf.getLocalDeletionTime());
        /** child chunks */
        sstmc.addColumnCount(addIColumns(true, true, 1, cf, iter));
    }

    /** Recursively encode IColumn instances, flushing them to disk when thresholds are reached. */
    private int addIColumns(boolean firstForParent, boolean lastForParent, int depth, IColumnContainer container, Iterator<IColumn> iter) throws IOException
    {
        // open the metadata range at this level
        openRangeMetadata(depth, null, container);
        int added = 0;
        boolean hasNext = iter.hasNext();
        while (hasNext)
        {
            // 1th through Nth column for a parent
            IColumn icol = iter.next();
            hasNext = iter.hasNext();
            // before appending, flush if thresholds are violated
            if (depth == 1)
            {
                // for simplicities sake, we don't split or index supercolumns
                if (firstForParent || (lastForParent && !hasNext))
                    // index without flushing
                    iwriter.add(icol.name(), dataFile.getFilePointer());
                else if (!firstForParent && blockSize > targetBlockSize)
                {
                    // if we have some content for this parent, and we're past the threshold, we should flush first
                    System.out.println("splitting a block! " + blockSize);
                    appendAndReset();
                    iwriter.add(icol.name(), dataFile.getFilePointer());
                    firstForParent = true;
                }
            }
            
            added += addIColumn(firstForParent, lastForParent && !hasNext, depth, icol);

            // we've satisfied our parent's desire to flush: reset and make our own decisions
            firstForParent = false;
        }
        // the end of this parent
        closeRangeMetadata(depth, null);
        return added;
    }

    private int addIColumn(boolean firstForParent, boolean lastForParent, int depth, IColumn icol) throws IOException
    {
        boolean isSuper = icol.getClass() == SuperColumn.class;
        addEntry(depth, icol.name(), isSuper ? Chunk.ENTRY_PARENT : Chunk.ENTRY_NAME);
        if (!isSuper)
            // standard value
            return addColumn(depth, (Column)icol);
        // supercolumn value: recurse
        SuperColumn scol = (SuperColumn)icol;
        return addIColumns(firstForParent, lastForParent, depth + 1, scol, scol.getSortedColumns().iterator());
    }

    private int addColumn(int depth, Column col) throws IOException
    {
        byte cm;
        clientTimestampAdd(depth + 1, col.timestamp());
        ByteBuffer value;
        if (col.getClass() == Column.class)
        {
            cm = Chunk.ENTRY_STANDARD;
            value = col.value();
        }
        else if (col.getClass() == DeletedColumn.class)
        {
            cm = Chunk.ENTRY_DELETED;
            chunks[depth + 1].localTimestampAdd(col.getLocalDeletionTime());
            value = null;
        }
        else if (col.getClass() == ExpiringColumn.class)
        {
            cm = Chunk.ENTRY_EXPIRING;
            // we don't record the TTL for expiring columns: see CASSANDRA-2620
            chunks[depth + 1].localTimestampAdd(col.getLocalDeletionTime());
            value = col.value();
        }
        else if (col.getClass() == CounterColumn.class)
        {
            cm = Chunk.ENTRY_COUNTER;
            CounterColumn ccol = (CounterColumn)col;
            clientTimestampAdd(depth + 1, ccol.timestampOfLastDelete());
            value = ccol.value();
        }
        else
            throw new RuntimeException("FIXME: " + col.getClass() + " not implemented");
        addEntry(depth + 1, value, cm);
        return 1;
    }

    private final void clientTimestampAdd(int depth, long clientTS)
    {
        sstmc.updateMaxTimestamp(clientTS);
        chunks[depth].clientTimestampAdd(clientTS);
    }

    /**
     * @return After adding an entry at the given depth, true if we need to flush so
     * that an entry can be added to the index.
     */
    private void addEntry(int depth, ByteBuffer entry, byte type)
    {
        int incby; // approximate uncompressed size of the entry
        if (entry == null)
            incby = 3;
        else
        {
            chunks[depth].values().add(entry);
            incby = entry.remaining() + 5;
        }
        blockSize += incby;
        addMeta(depth, type);
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
        byte type;
        if (start == null)
            type = Chunk.ENTRY_RANGE_BEGIN_NULL;
        else
        {
            type = Chunk.ENTRY_RANGE_BEGIN;
            chunks[depth].values().add(start);
        }
        clientTimestampAdd(depth, timestamp);
        chunks[depth].localTimestampAdd(localDeletionTime);
        addMeta(depth, type);
        // record the open range
        openType[depth] = type;
        openClient[depth] = timestamp;
        openLocal[depth] = localDeletionTime;
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
        }
        addMeta(depth, cfm);
        openType[depth] = Chunk.ENTRY_NULL;
    }

    private void addMeta(int depth, byte type)
    {
        chunks[depth].metadataAdd(type);
    }

    /**
     * Flush the current span and reset chunks for the next span.
     */
    private void appendAndReset() throws IOException
    {
        // stash state for any open ranges
        Stash[] stashed = maybeStashRanges();
        // flush and clear chunks from top to bottom
        for (int i = 0; i < chunks.length; i++)
        {
            Chunk chunk = chunks[i];
            chunk.append(dataFile);
            chunk.clear();
        }
        blockSize = 0;
        blocks++;
        // restore any open ranges
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
            // addEntry for parent if child is open
            if (i != 0)
                addEntry(i - 1, stashed[i - 1].val, Chunk.ENTRY_PARENT);
        }
    }

    /** Must be flushed before the underlying DataFile is closed. */
    public void flush() throws IOException
    {
        // close metadata at the key level
        closeRangeMetadata(0, null);
        appendAndReset();
        logger.info("Wrote " + blocks + " blocks with avg size " + (dataFile.getFilePointer() / blocks) + " bytes to " + dataFile);
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
