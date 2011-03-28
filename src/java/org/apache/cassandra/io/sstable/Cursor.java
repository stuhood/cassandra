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

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
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
    private final int time;

    public Cursor(Descriptor desc, List<AbstractType> types)
    {
        this(desc, types, false);
    }

    public Cursor(Descriptor desc, List<AbstractType> types, boolean fromRemote)
    {
        this.chunks = new Chunk[types.size()];
        this.fromRemote = fromRemote;
        this.time = (int)(System.currentTimeMillis() / 1000);
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
    
    /**
     * Shallow copy constructor: to perform a deep copy, create a shallow copy, and then call reset.
     */
    public Cursor(Cursor that)
    {
        this.fromRemote = that.fromRemote;
        this.time = that.time;

        this.meta = (int[])that.meta.clone();
        this.val = (int[])that.val.clone();
        this.local = (int[])that.local.clone();
        this.localNulls = (int[])that.localNulls.clone();
        this.client = (int[])that.client.clone();
        this.clientNulls = (int[])that.clientNulls.clone();

        // shallow copy the Chunks
        this.chunks = (Chunk[])that.chunks.clone();
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

    /** @return Key read from the first chunk and validated against @key if non-null */
    public DecoratedKey getKey(final IPartitioner p, DecoratedKey key)
    {
        byte keyMeta = getMeta(keyDepth());
        assert keyMeta == Chunk.ENTRY_PARENT :
            "Bad row key metadata '" + keyMeta + "' in " + this;
        if (key == null)
            return p.decorateKey(getVal(keyDepth()));

        // TODO: this is not fun: should hold an AbstractType per partitioner
        int res = searchAt(0, key.key, new Comparator<ByteBuffer>()
        {
            public int compare(ByteBuffer b1, ByteBuffer b2)
            {
                // assumes b1 is a value from the buffer, and b2 is the search term:
                // continue searching until we match or the buffer is consumed
                return b1.equals(b2) ? 0 : -1;
            }
        });
        ByteBuffer actual = getVal(keyDepth());
        assert actual.equals(key.key) : "Positioned at the wrong row! expected: " + key + " actual: " + p.decorateKey(actual);
        return key;
    }

    /**
     * Gets the range metadata for a parent from a chunk: always the first
     * entry at the child level for a particular level.
     */
    public void applyMetadata(int depth, IColumnContainer container)
    {
        RangeEntry range = getMetadata(depth);
        container.markForDeleteAt(range.localDeletionTime, range.markedForDeleteAt);
    }

    public RangeEntry getMetadata(int depth)
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
                // fall through
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

    /**
     * Builds a Column from the current position of the cursor, or null
     * for the end of the parent.
     */
    private Column getStandard()
    {
        byte nm = getMeta(nameDepth());
        ByteBuffer name;
        if (nm == Chunk.ENTRY_NAME)
            name = getVal(nameDepth());
        else
            throw new AssertionError("Invalid type for column name: " + nm);
        byte cm = getMeta(valueDepth());
        Column col = null;
        switch (cm)
        {
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
                col = ExpiringColumn.create(name,
                                            getVal(valueDepth()),
                                            getClient(valueDepth()),
                                            ttl,
                                            localExpirationTime,
                                            time);
                break;
            case Chunk.ENTRY_COUNTER:
                col = CounterColumn.create(name,
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
                if (isSentinelAt(nameDepth())) return endOfData();
                return getStandard();
            }
        };

        // build the supercolumn
        byte cfm = getMeta(superNameDepth());
        if (cfm != Chunk.ENTRY_PARENT)
            throw new AssertionError("Invalid type for super column name: " + cfm);
        ByteBuffer name = getVal(superNameDepth());
        // read 1) beginning of supercolumn range, 2) columns, 3) end of range
        RangeEntry begin = getMetadata(nameDepth());
        SuperColumn scol = new SuperColumn(name, chunks[nameDepth()].type, iter);
        RangeEntry end = getMetadata(nameDepth());
        scol.markForDeleteAt(begin.localDeletionTime, begin.markedForDeleteAt);
        assert end.value == null : "SuperColumns may not be split across blocks!";
        return scol;
    }

    /**
     * @return A BlockHeader representing a summary of the current span.
     */
    public BlockHeader summarizeSpan(long position)
    {
        List<ByteBuffer> values = chunks[1].values();
        // TODO: arbitrary
        if (values.size() < 32)
            return new BlockHeader(position);
        RangeEntry meta = getMetadata(1);
        ByteBuffer min = values.get(0);
        ByteBuffer max = values.get(values.size() - 1);
        return new NestedBlockHeader(position,
                                   meta.markedForDeleteAt,
                                   meta.localDeletionTime,
                                   min,
                                   max);
    }

    /**
     * Reads the next span of Chunks into this cursor.
     */
    public void nextSpan(DataInput file) throws IOException
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
     * @return The type of the next entry less than or equal to the given value
     * for the current parents. If 'value' is null, any entry will be accepted.
     * If there are no matching entries, ENTRY_NULL will be returned.
     */
    public byte peekAt(int depth, ByteBuffer value)
    {
        assert isAvailable() : "Cursor not open";

        ByteBuffer metadata = chunks[depth].metadata();
        byte m = metadata.get(metadata.position() + meta[depth]);
        if (isSentinel(m))
            // reached end of parent: no point in comparing
            return m;
        if (value == null)
            // positioned at a value type, with an unbounded range
            return m;
        // peek at the value and compare
        ByteBuffer cur = chunks[depth].values().get(val[depth]);
        if (chunks[depth].type.compare(value, cur) < 0)
            // no more values in the window lte val
            return Chunk.ENTRY_NULL;
        // current entry matches
        return m;
    }

    /**
     * Increments the current position to the first position greater than or equal to 'val'. A
     * null value will scan to the first sentinel value.
     * @return 0 if an exact match was found, greater than 0 if a GT match was found, less than 0
     * if a sentinel value was reached.
     */
    public int searchAt(int depth, ByteBuffer value)
    {
        return searchAt(depth, value, chunks[depth].type);
    }

    public int searchAt(int depth, ByteBuffer value, Comparator<ByteBuffer> comparator)
    {
        // sequential search for a value, bounded by a sentinel
        while (!isSentinelAt(depth))
        {
            // compare to current value
            ByteBuffer cur = chunks[depth].values().get(val[depth]);
            int comp = comparator.compare(cur, value);
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

    /**
     * Skips up to the sentinel value for the current parent of depth.
     * TODO: separate lazy decoding of Chunks so this doesn't deserialize all content
     */
    public byte skipAllAt(int depth)
    {
        // sequential search for a value, bounded by a sentinel
        while (!isSentinelAt(depth))
            skipAt(depth);
        return peekAt(depth, null);
    }

    /** @return True if we're positioned at a sentinel type at depth. */
    public boolean isSentinelAt(int depth)
    {
        ByteBuffer metadata = chunks[depth].metadata();
        return isSentinel(metadata.get(metadata.position() + meta[depth]));
    }

    private boolean isSentinel(byte type)
    {
        switch (type)
        {
            case Chunk.ENTRY_RANGE_END:
            case Chunk.ENTRY_NULL:
            case Chunk.ENTRY_RANGE_END_NULL:
                return true;
            default:
                return false;
        }
    }

    /**
     * Recursively skips all content for one entry at depth.
     */
    public void skipAt(int depth)
    {
        byte m = getMeta(depth, meta[depth]++);
        switch (m)
        {
            case Chunk.ENTRY_NAME:
                // name with exactly one child
                val[depth]++;
                skipAt(depth + 1);
                break;
            case Chunk.ENTRY_PARENT:
                // parent with children
                val[depth]++;
                // skip the children
                while (!isSentinelAt(depth + 1))
                    skipAt(depth + 1);
                // ...and the sentinel
                skipAt(depth + 1);
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

    private ByteBuffer getVal(int depth)
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

    public boolean isAvailable()
    {
        if (meta[1] == -1)
            // not open
            return false;
        // has more entries?
        return chunks[1].metadata().remaining() > meta[1];
    }

    public void clear()
    {
        for (Chunk chunk : chunks)
            chunk.clear();
    }

    /**
     * Deep copy that cursor into this cursor: this Cursor must already be a
     * shallow clone of that cursor.
     * TODO: this is necessary to support 'reset' on ChunkedIdentityIterator, but
     * anything we can do to optimize it away would be fantastic: in particular,
     * when we no longer need to support two pass compaction, we can remove
     * SSTableIdentityIterator.reset() and both copy-related methods on Cursor.
     */
    public Cursor reset(Cursor that)
    {
        assert this.time == that.time && this.fromRemote == that.fromRemote;
        intCopy(that.meta, this.meta);
        intCopy(that.val, this.val);
        intCopy(that.local, this.local);
        intCopy(that.localNulls, this.localNulls);
        intCopy(that.client, this.client);
        intCopy(that.clientNulls, this.clientNulls);
        assert this.chunks.length == that.chunks.length;
        for (int i = 0; i < that.chunks.length; i++)
            this.chunks[i] = new Chunk(that.chunks[i]);
        return this;
    }

    private static void intCopy(int[] src, int[] dest)
    {
        assert src.length == dest.length;
        System.arraycopy(src, 0, dest, 0, src.length);
    }

    public String toString()
    {
        ArrayList<String> mstrs = new ArrayList<String>();
        for (Chunk chunk : chunks)
            mstrs.add(chunk.metadata().remaining() + "");
        return "Cursor(" + Arrays.toString(meta) + ", " + mstrs + ")";
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
