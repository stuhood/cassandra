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

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A mutable object representing a precise position in an SSTable, with methods for
 * decoding data from Chunks.
 */
public final class Cursor
{
    private final Chunk[] chunks;
    // the position of the cursor in the metadata and values for each level of
    // a span: each level points to the current child of the parent in the level
    // above
    private final int[] meta;
    private final int[] val;
    private final int[] local;
    private final int[] localNulls;
    private final int[] client;
    private final int[] clientNulls;

    private final boolean fromRemote;
    private final int time = (int)(System.currentTimeMillis() / 1000);

    public Cursor(Descriptor desc, List<AbstractType> types)
    {
        this(desc, types, false);
    }

    public Cursor(Descriptor desc, List<AbstractType> types, boolean fromRemote)
    {
        this.chunks = new Chunk[types.size()];
        this.fromRemote = fromRemote;
        // the index in the last 2 chunks is always the same
        for (int i = 0; i < types.size(); i++)
            this.chunks[i] = new Chunk(desc, types.get(i));
        this.meta = new int[types.size()];
        this.val = new int[types.size()];
        this.local = new int[types.size()];
        this.localNulls = new int[types.size()];
        this.client = new int[types.size()];
        this.clientNulls = new int[types.size()];
        for (int[] arr : Arrays.asList(meta, val, local, localNulls, client, clientNulls))
            Arrays.fill(arr, -1);
    }
    
    public int keyDepth()
    {
        return 0;
    }

    public int superNameDepth()
    {
        assert chunks.length == 4;
        return 1;
    }

    public int nameDepth()
    {
        return chunks.length - 2;
    }

    public int valueDepth()
    {
        return chunks.length - 1;
    }

    /**
     * Gets the range metadata for a parent from a chunk: always the first
     * entry at the child level for a particular parent.
     */
    public void applyMetadata(int depth, IColumnContainer container)
    {
        RangeEntry range = getMetadata(depth);
        container.markForDeleteAt(range.localDeletionTime, range.markedForDeleteAt);
    }

    private RangeEntry getMetadata(int depth)
    {
        ByteBuffer val = null;
        int localDeletionTime = Integer.MIN_VALUE;
        long markedForDeleteAt = Long.MIN_VALUE;
        byte cfm = getMeta(depth);
        switch(cfm)
        {
            case Chunk.ENTRY_RANGE_BEGIN:
                // begin of partial range
                val = getVal(depth);
                // fall through
            case Chunk.ENTRY_RANGE_BEGIN_NULL:
                localDeletionTime = getLocal(depth);
                markedForDeleteAt = getClient(depth);
                break;
            case Chunk.ENTRY_RANGE_END:
                // end of partial range
                val = getVal(depth);
                break;
            case Chunk.ENTRY_RANGE_END_NULL:
                break;
            default:
                throw new RuntimeException("Unsupported range metadata type: " + cfm);
        }
        return new RangeEntry(val, localDeletionTime, markedForDeleteAt);
    }

    public IColumn getColumn()
    {
        return chunks.length == 3 ? getStandard() : getSuper();
    }

    /** Builds a Column from the current position of the cursor. */
    private Column getStandard()
    {
        //System.out.println("\nGetting column from " + cursor);
        byte nm = getMeta(nameDepth());
        ByteBuffer name;
        if (nm == Chunk.ENTRY_NAME)
            name = getVal(nameDepth());
        else if (nm == Chunk.ENTRY_NULL)
            name = null;
        else
            throw new AssertionError("Invalid type for column name: " + nm);
        //System.out.println("\tGetting name from chunk: " + chunks[nameDepth()]);
        //System.out.println("\tGot name from " + cursor);
        //System.out.println("\tGetting value from chunk: " + chunks[valueDepth()]);
        byte cm = getMeta(valueDepth());
        Column col = null;
        switch (cm)
        {
            case Chunk.ENTRY_NULL:
                col = null; // null columns indicate parents without children
                break;
            case Chunk.ENTRY_STANDARD:
                col = new Column(name,
                                getVal(valueDepth()),
                                getClient(valueDepth()));
                break;
            case Chunk.ENTRY_DELETED:
                col = new DeletedColumn(name,
                                        getLocal(valueDepth()),
                                        getClient(valueDepth()));
                break;
            case Chunk.ENTRY_EXPIRING:
                int localExpirationTime = getLocal(valueDepth());
                // calculate a delta TTL for expiring columns rather than storing it
                int ttl = localExpirationTime - time;
                col = ExpiringColumn.get(name,
                                         getVal(valueDepth()),
                                         getClient(valueDepth()),
                                         ttl,
                                         localExpirationTime,
                                         time);
                break;
            case Chunk.ENTRY_COUNTER:
                col = CounterColumn.get(name,
                                        getVal(valueDepth()),
                                        getClient(valueDepth()),
                                        getClient(valueDepth()),
                                        fromRemote);
                break;
            default:
                throw new RuntimeException("Decoding not implemented for: " + cm);
        }
        return col;
    }

    /** Builds a SuperColumn from the current position of the cursor. */
    private SuperColumn getSuper()
    {
        // collect all children for the current parent
        Iterator<IColumn> iter = new AbstractIterator<IColumn>()
        {
            @Override
            public IColumn computeNext()
            {
                if (isConsumed(nameDepth(), null)) return endOfData();
                return getStandard();
            }
        };

        // build the supercolumn
        byte cfm = getMeta(superNameDepth());
        if (cfm != Chunk.ENTRY_PARENT)
            throw new AssertionError("Invalid type for super column name: " + cfm);
        ByteBuffer name = getVal(superNameDepth());
        RangeEntry begin = getMetadata(2);
        SuperColumn scol = new SuperColumn(name, chunks[nameDepth()].type, iter);
        RangeEntry end = getMetadata(2);
        scol.markForDeleteAt(begin.localDeletionTime, begin.markedForDeleteAt);
        assert end.value == null : "SuperColumns may not be split across blocks!";
        return scol;
    }

    /**
     * Reads the next span of Chunks into this cursor.
     */
    public void nextSpan(FileDataInput file) throws IOException
    {
        for (int i = 0; i < chunks.length; i++)
            chunks[i].next(file);
        Arrays.fill(this.meta, 0);
        Arrays.fill(this.val, 0);
        Arrays.fill(this.client, 0);
        Arrays.fill(this.clientNulls, 0);
        Arrays.fill(this.local, 0);
        Arrays.fill(this.localNulls, 0);
    }

    /**
     * @return True if the cursor is not open, or if the current span indicates
     * that there are more spans for the current parent.
     */
    public boolean hasMoreSpans()
    {
        if (meta[0] == -1)
            // not open
            return true;
        // if the last entry (at any level) in the span is a non-null range
        // split, it indicates that the parent has more content
        for (int i = 0; i < chunks.length; i++)
            if (getMeta(i, chunks[i].metadata().remaining() - 1) == Chunk.ENTRY_RANGE_END)
                return true;
        return false;
    }

    /**
     * @return True if all values less than or equal to the given value in the window
     * have been consumed. A null val means unbounded.
     */
    public boolean isConsumed(int depth, ByteBuffer value)
    {
        if (!peekVal(depth))
            // fully consumed, or only the range metadata remains
            return true;
        if (value == null)
            // positioned at a value type, with an unbounded range
            return false;
        ByteBuffer cur = chunks[depth].values().get(val[depth]);
        if (chunks[depth].type.compare(value, cur) < 0)
            // no more values in the window lte val
            return true;
        return false;
    }

    /**
     * Increments the current position to the first position greater than or equal to 'val'.
     * @return 0 if an exact match was found, greater than 0 if a GT match was found, less than 0 otherwise.
     */
    public int searchAt(int depth, ByteBuffer value)
    {
        // sequential search for a value, bounded by non-value entries: we assume here (and
        // in isConsumed) that there is a Null/RangeEnd entry at the end of the window
        while (peekVal(depth))
        {
            // System.out.println("\t\tvalueSearch m=" + midx[depth] + ", v=" + vidx[depth] + " of " + maxVidx);
            // compare to current value
            ByteBuffer cur = chunks[depth].values().get(val[depth]);
            //System.out.println("\t\tComparing to" + new String(cur.array()));
            int comp = chunks[depth].type.compare(cur, value);
            if (comp < 0)
            {
                // no match
                skipAt(depth);
                continue;
            }
            return comp;
        }
        // reached end of window
        return -1;
    }

    /** @return True if there is a value type at depth. */
    private boolean peekVal(int depth)
    {
        assert depth < valueDepth();
        ByteBuffer metadata = chunks[depth].metadata();
        byte m = metadata.get(metadata.position() + meta[depth]);
        switch (m)
        {
            case Chunk.ENTRY_NAME:
            case Chunk.ENTRY_PARENT:
            case Chunk.ENTRY_STANDARD:
            case Chunk.ENTRY_EXPIRING:
            case Chunk.ENTRY_COUNTER:
                return true;
            case Chunk.ENTRY_RANGE_END:
            case Chunk.ENTRY_RANGE_BEGIN:
            case Chunk.ENTRY_NULL:
            case Chunk.ENTRY_DELETED:
            case Chunk.ENTRY_RANGE_BEGIN_NULL:
            case Chunk.ENTRY_RANGE_END_NULL:
                return false;
            default:
                throw new AssertionError("Unsupported metadata type: " + m);
        }
    }

    /**
     * Recursively skips all content for one entry at depth.
     */
    private void skipAt(int depth)
    {
        //System.out.println("Skipping at " + depth + ": m" + meta[depth] + " v" + val[depth]);
        byte m = getMeta(depth, meta[depth]++);
        switch (m)
        {
            case Chunk.ENTRY_NAME:
                // name with exactly one child
                val[depth]++;
                skipAt(depth + 1);
                break;
            case Chunk.ENTRY_PARENT:
                // name with children
                while (!isConsumed(depth + 1, null))
                    skipAt(depth + 1);
                // assume a range-end
                skipAt(depth + 1);
                val[depth]++;
                break;
            case Chunk.ENTRY_RANGE_END:
                // value with no timestamps
                val[depth]++;
                break;
            case Chunk.ENTRY_STANDARD:
                // value and a client timestamp
                val[depth]++;
                skipClientAt(depth);
                break;
            case Chunk.ENTRY_EXPIRING:
            case Chunk.ENTRY_RANGE_BEGIN:
                // a value with client and user timestamps
                val[depth]++;
                skipClientAt(depth);
                skipLocalAt(depth);
                break;
            case Chunk.ENTRY_COUNTER:
                // a value and two client timestamps
                val[depth]++;
                skipClientAt(depth);
                skipClientAt(depth);
                break;
            case Chunk.ENTRY_DELETED:
            case Chunk.ENTRY_RANGE_BEGIN_NULL:
                // no value: just timestamps
                skipClientAt(depth);
                skipLocalAt(depth);
                break;
            case Chunk.ENTRY_NULL:
            case Chunk.ENTRY_RANGE_END_NULL:
                // no content at all
                break;
            default:
                throw new AssertionError("Unsupported metadata type: " + m);
        }
    }

    private void skipClientAt(int depth)
    {
        if (chunks[depth].clientTimestampNulls().get(clientNulls[depth]++))
            // non-null
            client[depth]++;
    }

    private void skipLocalAt(int depth)
    {
        if (chunks[depth].localTimestampNulls().get(localNulls[depth]++))
            // non-null
            local[depth]++;
    }

    public ByteBuffer getVal(int depth)
    {
        return chunks[depth].values().get(val[depth]++);
    }

    private byte getMeta(int depth)
    {
        return getMeta(depth, meta[depth]++);
    }

    private byte getMeta(int depth, int index)
    {
        ByteBuffer metadata = chunks[depth].metadata();
        return metadata.get(metadata.position() + index);
    }

    private long getClient(int depth)
    {
        if (chunks[depth].clientTimestampNulls().get(clientNulls[depth]++))
            return chunks[depth].clientTimestamps().get(client[depth]++);
        return Long.MIN_VALUE;
    }

    private int getLocal(int depth)
    {
        if (chunks[depth].localTimestampNulls().get(localNulls[depth]++))
            return chunks[depth].localTimestamps().get(local[depth]++);
        return Integer.MIN_VALUE;
    }

    public String toString()
    {
        ArrayList<Integer> msizes = new ArrayList<Integer>();
        for (Chunk chunk : chunks)
            msizes.add(chunk.metadata().remaining());
        return "Cursor(" + Arrays.toString(meta) + ", " + msizes + ")";
    }

    private static final class RangeEntry
    {
        public final ByteBuffer value;
        public final int localDeletionTime;
        public final long markedForDeleteAt;
        private RangeEntry(ByteBuffer value, int localDeletionTime, long markedForDeleteAt)
        {
            this.value = value;
            this.localDeletionTime = localDeletionTime;
            this.markedForDeleteAt = markedForDeleteAt;
        }
    }
}
