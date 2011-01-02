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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.nio.ByteBuffer;

import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.avro.*;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;

/**
 * Utility class for deserializing (I)Columns from Chunks in a random-access fashion.
 */
public final class Chunks
{
    private Chunks(){}

    /** Gets the range metadata for a parent from a chunk. */
    public static void applyMetadata(Chunk chunk, IColumnContainer container, int idx)
    {
        // TODO: assumes that parents can only store one range's worth of metadata
        ChunkFieldMetadata cfm = chunk.range_metadata.get(idx);
        if (cfm.type == ChunkFieldType.NULL)
            // range has "default" metadata: pass
            return;
        else if (cfm.type == ChunkFieldType.DELETED)
            // is deleted
            container.markForDeleteAt(cfm.localDeletionTime, cfm.timestamp);
        else
            throw new RuntimeException("Unsupported range metadata type: " + cfm);
    }

    public static IColumn getColumn(Cursor cursor, CFMetaData meta)
    {
        if (cursor.chunks.length == 3)
            // standard column family
            return Chunks.getValue(cursor);
        return Chunks.getSuper(cursor, meta.subcolumnComparator);
    }

    /** Deserializes a Column from the last two chunks of a span and increments the cursor. */
    public static Column getValue(Cursor cursor)
    {
        int idx = cursor.getAndInc();
        Chunk nameChunk = cursor.chunks[cursor.chunks.length - 2];
        Chunk valueChunk = cursor.chunks[cursor.chunks.length - 1];
        ByteBuffer name = nameChunk.values.get(idx);
        ChunkFieldMetadata cm = valueChunk.value_metadata.get(idx);
        switch (cm.type)
        {
            case STANDARD:
                return new Column(name, valueChunk.values.get(idx), cm.timestamp);
            case DELETED:
                return new DeletedColumn(name, cm.localDeletionTime, cm.timestamp);
            case EXPIRING:
                return new ExpiringColumn(name, valueChunk.values.get(idx), cm.timestamp, cm.timeToLive, cm.localDeletionTime);
            case COUNTER:
                return new CounterColumn(name, valueChunk.values.get(idx), cm.timestamp, cm.timestampOfLastDelete);
            case NULL:
                return null; // null columns indicate parents without children
        }
        throw new RuntimeException("FIXME: " + cm.type + " not implemented");
    }

    /** Builds a SuperColumn from the current position of the given cursor. */
    public static SuperColumn getSuper(final Cursor cursor, AbstractType comparator)
    {
        int count = Chunks.childCount(cursor.chunks[2], cursor.indexes[2]);
        final int end = cursor.indexes[2] + count;
        // collect 'count' children starting from 'idx'
        // filter null columns, which indicate that the supercolumn was a tombstone
        Iterator<IColumn> iter = Iterators.filter(new AbstractIterator<IColumn>()
        {
            @Override
            public IColumn computeNext()
            {
                if (cursor.indexes[2] >= end) return endOfData();
                return Chunks.getValue(cursor);
            }
        }, Predicates.notNull());

        // build the supercolumn
        int idx = cursor.getAndIncAt(1);
        ByteBuffer name = cursor.chunks[1].values.get(idx);
        SuperColumn scol = new SuperColumn(name, comparator, count, iter);
        Chunks.applyMetadata(cursor.chunks[2], scol, idx);
        return scol;
    }

    /**
     * @return The count of (consecutive) children with the same parent (flag).
     * TODO: The intention is to replace this with a bit field, hence the odd approach.
     */
    private static int childCount(Chunk chunk, int idx)
    {
        // all children of a parent share one "parentFlag" value:
        // fetch it from the first child, and compare to consecutive children
        boolean parentFlag = chunk.value_metadata.get(idx).parent;
        int i = idx;
        for (; i < chunk.value_metadata.size(); i++)
            if (chunk.value_metadata.get(i).parent != parentFlag)
                break;
        return i - idx;
    }

    /** A mutable object representing a position in the chunks of a span. */
    public static final class Cursor
    {
        public final Chunk[] chunks;
        // the position of the cursor in each level of a span
        public final int[] indexes;

        public Cursor(boolean isSuper)
        {
            this(isSuper ? 4 : 3);
        }

        public Cursor(int depth)
        {
            this.chunks = new Chunk[depth];
            // the index in the last 2 chunks is always the same
            this.indexes = new int[depth - 1];
        }

        public void reset()
        {
            Arrays.fill(this.chunks, null);
            Arrays.fill(this.indexes, 0);
        }

        /**
         * @return True if the cursor is not open, or if the current span indicates
         * that there are more spans for the current parent.
         */
        public boolean hasMoreSpans()
        {
            if (chunks[0] == null)
                // not open
                return true;
            // see if ranges indicate that the parent has more content: parents can
            // only have 1 metadata range per span (at least until we implement
            // range/slice deletes)
            ByteBuffer right = chunks[1].ranges.get(indexes[0] * 2 + 1);
            return !EMPTY_BYTE_BUFFER.equals(right);
        }

        /**
         * @return True if the cursor points to a tombstone, or is past rhs at depth.
         */
        public boolean isConsumed(int depth, int rhs)
        {
            // is past rhs?
            if (indexes[depth] >= rhs) return true;
            // is a tombstone?
            return ChunkFieldType.DELETED == chunks[depth].value_metadata.get(indexes[depth]).type;
        }

        /**
         * Seeks to the first child of the current parent at depth. For example: seek
         * to the first subcolumn of the current super column (at 'depth' = 2).
         */
        public void seekToFirstChild(int depth)
        {
            // looking for the first child of the nth parent
            int n = indexes[depth];
            int cd = depth + 1;
            if (n == -1)
            {
                // no child
                indexes[cd] = -1;
                return;
            }
            Chunk cc = chunks[cd];
            int count = 0;
            boolean parentFlag = !cc.value_metadata.get(0).parent;
            for (indexes[cd] = 0; indexes[cd] < cc.value_metadata.size(); indexes[cd]++)
            {
                if (cc.value_metadata.get(indexes[cd]).parent == parentFlag)
                    continue;
                if (count++ == n)
                    return;
                parentFlag = cc.value_metadata.get(indexes[cd]).parent;
            }
            assert false : "Parent " + n + " in " + chunks[depth] + " did not have a child in " + chunks[cd];
        }

        public int binSearch(int depth, ByteBuffer val, AbstractType comp)
        {
            return Collections.binarySearch(chunks[depth].values, val, comp);
        }

        /**
         * Performs a binary search at the given depth, and updates the index
         * to the first position greater than or equal to 'val'.
         */
        public void searchAt(int depth, ByteBuffer val, AbstractType comp)
        {
            int idx = binSearch(depth, val, comp);
            if (idx < 0)
                idx = -idx - 1; // name at insertion position
            indexes[depth] = idx;
        }

        /** Get and increment the index at the given depth. */
        public int getAndIncAt(int depth)
        {
            return indexes[depth]++;
        }

        /** Get and increment the index at the maximum depth. */
        public int getAndInc()
        {
            return getAndIncAt(indexes.length - 1);
        }

        public String toString()
        {
            return "Cursor" + Arrays.toString(indexes);
        }
    }
}
