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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.nio.ByteBuffer;

import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.Schema;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.io.sstable.avro.*;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;

/**
 * Tracks the state and thresholds necessary to Chunk an iterator nested at depth N
 * into a Span of N levels/chunks.
 *
 * We force a 'sync' of the underlying file after every appended object, meaning
 * that they are compressed individually, and must consequently be reasonably
 * large to make the compression worthwhile.
 */
class ChunkAppender
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private static final Schema BYTE_BUFFER_SCHEMA = Schema.createArray(Schema.parse("{\"type\": \"bytes\"}"));

    private final DataFileWriter<Chunk> dataFile;
    // arrays of length "chunksPerSpan" containing the current chunk, parent flag,
    // and whether a metadata range is open at each level
    private final Chunk[] chunks;
    private final boolean[] parents;
    private final boolean[] isOpen;
    private long spanBytes;

    // target size of a span, in bytes
    private final long targetSpanBytes;

    public ChunkAppender(DataFileWriter<Chunk> dataFile, int chunksPerSpan, int targetSpanSizeInKB)
    {
        this.targetSpanBytes = targetSpanSizeInKB * 1024;
        this.dataFile = dataFile;
        this.chunks = new Chunk[chunksPerSpan];
        this.parents = new boolean[chunksPerSpan];
        this.isOpen = new boolean[chunksPerSpan];
        for (int i = 0; i < chunksPerSpan; i++)
        {
            Chunk chunk = new Chunk();
            chunk.values = SerDeUtils.<ByteBuffer>createArray(256, BYTE_BUFFER_SCHEMA);
            chunk.ranges = SerDeUtils.<ByteBuffer>createArray(256, BYTE_BUFFER_SCHEMA);
            chunk.value_metadata = SerDeUtils.<ChunkFieldMetadata>createArray(256, ChunkFieldMetadata.SCHEMA$);
            chunk.range_metadata = SerDeUtils.<ChunkFieldMetadata>createArray(256, ChunkFieldMetadata.SCHEMA$);
            chunk.chunk = i;
            chunks[i] = chunk;
            parents[i] = false;
            isOpen[i] = false;
        }
    }

    public int append(DecoratedKey decoratedKey, ColumnFamily cf, Iterator<IColumn> iter) throws IOException
    {
        /** key chunk */
        ChunkFieldMetadata km = new ChunkFieldMetadata();
        km.type = ChunkFieldType.STANDARD;
        add(0, decoratedKey.key, km);

        /** child chunks */
        // add child chunks recursively
        int columnCount = addTo(1, cf, iter);

        // force append to finish the row
        // TODO: removing this force would mean we would start storing more than one row per span
        appendAndReset();
        return columnCount;
    }

    /** Flip the parent flag for the given depth, and return it. */
    private boolean parentFlipAndGet(int depth)
    {
        return parents[depth] = !parents[depth];
    }

    /** Recursively encode IColumn instances, flushing them to disk when thresholds are reached. */
    private int addTo(int depth, IColumnContainer container, Iterator<IColumn> iter) throws IOException
    {
        // the parent for this level has changed to 'container': toggle parent flag
        boolean parentFlag = parentFlipAndGet(depth);
        int columnCount = 0;
        // open the metadata range at this level
        openRangeMetadata(depth, EMPTY_BYTE_BUFFER, container, parentFlag);
        if (!iter.hasNext())
        {
            // this parent has no children: must contain a null child entry at every level
            ChunkFieldMetadata nm = new ChunkFieldMetadata();
            nm.parent = parentFlag;
            nm.type = ChunkFieldType.NULL;
            add(depth, EMPTY_BYTE_BUFFER, nm);
            // TODO: we shouldn't store a value for nulls: doing it for convenience
            for (int i = depth + 1; i < chunks.length; i++)
            {
                ChunkFieldMetadata cm = new ChunkFieldMetadata();
                cm.parent = parentFlipAndGet(i);
                cm.type = ChunkFieldType.NULL;
                add(i, EMPTY_BYTE_BUFFER, cm);
            }
        }
        while (iter.hasNext())
        {
            // before appending new columns, see if thresholds are violated
            maybeAppendAndReset();

            IColumn icol = iter.next();
            /** name level */
            ChunkFieldMetadata nm = new ChunkFieldMetadata();
            nm.parent = parentFlag;
            nm.type = ChunkFieldType.STANDARD;
            add(depth, icol.name(), nm);

            /** value level */
            // supercolumn value: recurse
            if (icol.getClass() == SuperColumn.class)
            {
                // recurse
                SuperColumn scol = (SuperColumn)icol;
                addTo(depth + 1, scol, scol.getSortedColumns().iterator());
                continue;
            }
            // standard value
            ChunkFieldMetadata cm = new ChunkFieldMetadata();
            cm.timestamp = icol.timestamp();
            cm.parent = parentFlipAndGet(depth + 1);
            ByteBuffer value;
            if (icol.getClass() == Column.class)
            {
                cm.type = ChunkFieldType.STANDARD;
                value = icol.value();
            }
            else if (icol.getClass() == DeletedColumn.class)
            {
                cm.type = ChunkFieldType.DELETED;
                cm.localDeletionTime = icol.getLocalDeletionTime();
                // TODO: shouldn't store value for nulls
                value = EMPTY_BYTE_BUFFER;
            }
            else if (icol.getClass() == ExpiringColumn.class)
            {
                ExpiringColumn col = (ExpiringColumn)icol;
                cm.type = ChunkFieldType.EXPIRING;
                cm.timeToLive = col.getTimeToLive();
                cm.localDeletionTime = col.getLocalDeletionTime();
                value = icol.value();
            }
            else if (icol.getClass() == CounterColumn.class)
            {
                CounterColumn col = (CounterColumn)icol;
                cm.type = ChunkFieldType.COUNTER;
                cm.timestampOfLastDelete = col.timestampOfLastDelete();
                value = icol.value();
            }
            else
                throw new RuntimeException("FIXME: " + icol.getClass() + " not implemented");
            add(depth + 1, value, cm);
            columnCount++;
        }
        // the end of this parent
        closeRangeMetadata(depth, EMPTY_BYTE_BUFFER);
        maybeAppendAndReset();
        return columnCount;
    }

    private void add(int depth, ByteBuffer value, ChunkFieldMetadata cfm)
    {
        chunks[depth].values.add(value);
        chunks[depth].value_metadata.add(cfm);
        // FIXME: rough approx of metadata size: we'll be packing it sometime soon anyway
        spanBytes += value.remaining() + 5;
    }

    /** Adds range metadata to a chunk. */
    private void openRangeMetadata(int depth, ByteBuffer start, IColumnContainer container, boolean parentFlag)
    {
        assert !isOpen[depth] : "Range already open in " + chunks[depth];
        ChunkFieldMetadata cfm = new ChunkFieldMetadata();
        cfm.parent = parentFlag;
        if (container.isMarkedForDelete())
        {
            // is deleted
            cfm.type = ChunkFieldType.DELETED;
            cfm.timestamp = container.getMarkedForDeleteAt();
            cfm.localDeletionTime = container.getLocalDeletionTime();
        }
        else
            // no metadata: store as null
            // FIXME: note that we've stored range placeholders: once we are packing
            // metadata we can skip null ranges completely
            cfm.type = ChunkFieldType.NULL;
        openRangeMetadata(depth, start, cfm);
    }

    private void openRangeMetadata(int depth, ByteBuffer start, ChunkFieldMetadata cfm)
    {
        // left...
        chunks[depth].ranges.add(start);
        chunks[depth].range_metadata.add(cfm);
        isOpen[depth] = true;
        // FIXME: rough approx of metadata size: we'll be packing it sometime soon anyway
        spanBytes += start.remaining() + 5;
    }

    private void closeRangeMetadata(int depth, ByteBuffer end)
    {
        Chunk chunk = chunks[depth];
        assert isOpen[depth] : "Range not open in " + chunk;
        // ...right
        chunk.ranges.add(end);
        isOpen[depth] = false;
        spanBytes += end.remaining();
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
         * We need to preserve any open ranges w/ parents but we'd like to avoid
         * allocating new chunks after each flush, so we use temporary variables.
         */
        ByteBuffer[] splits = null, parents = null;
        ChunkFieldMetadata[] splitsMeta = null, parentsMeta = null;
        for (int i = 1; i < chunks.length; i++)
        {
            if (!isOpen[i])
                continue;
            // split the open range, asserting that it is not empty
            // TODO: to avoid doing this to too many rows (causing fragmentation), we should
            // eagerly close spans when we finish a key within ~75% of the threshold
            if (splits == null)
            {
                // lazily allocate
                int depth = chunks.length;
                splits = new ByteBuffer[depth];
                parents = new ByteBuffer[depth];
                splitsMeta = new ChunkFieldMetadata[depth];
                parentsMeta = new ChunkFieldMetadata[depth];
            }
            Chunk chunk = chunks[i];
            int valueCount = chunk.value_metadata.size();
            int rangeCount = chunk.range_metadata.size();
            assert chunk.value_metadata.get(valueCount - 1).parent == chunk.range_metadata.get(rangeCount - 1).parent :
                "Uh oh. The value parent disagrees with the range parent. Is the range empty? " + chunk;
            // split the range on the last appended value
            splits[i] = chunk.values.get(valueCount - 1);
            splitsMeta[i] = chunk.range_metadata.get(rangeCount - 1);
            // store the parent value
            parents[i - 1] = chunks[i - 1].values.get(chunks[i - 1].values.size() - 1);
            parentsMeta[i - 1] = chunks[i - 1].value_metadata.get(chunks[i - 1].values.size() - 1);
            closeRangeMetadata(i, splits[i]);
        }
        /**
         * Flush and clear chunks from top to bottom
         */
        for (int i = 0; i < chunks.length; i++)
        {
            Chunk chunk = chunks[i];
            // append and force a sync: @see class javadoc
            dataFile.append(chunk);
            dataFile.sync();
            chunk.values.clear();
            chunk.value_metadata.clear();
            chunk.ranges.clear();
            chunk.range_metadata.clear();
        }
        spanBytes = 0;
        /**
         * Restore any ranges that were open.
         */
        if (splits == null)
            return;
        for (int i = 1; i < chunks.length; i++)
        {
            if (splits[i] == null)
                continue;
            // restore the range
            openRangeMetadata(i, splits[i], splitsMeta[i]);
            // and the parent value
            add(i - 1, parents[i - 1], parentsMeta[i - 1]);
        }
    }

    /** @return True if internal buffers are empty. */
    public boolean isFlushed()
    {
        if (chunks[0].value_metadata.isEmpty() && chunks[0].range_metadata.isEmpty())
            return true;
        return false;
    }

    /** Must be flushed before the underlying DataFile is closed. */
    public void flush() throws IOException
    {
        appendAndReset();
    }
}
