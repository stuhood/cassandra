package org.apache.cassandra.io.sstable;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.obs.OpenBitSet;

/**
 * An object that observes and records tuples when thresholds are met. Tracks count
 * and size thresholds.
 */
public abstract class Observer
{
    // the size of entries since the last observed value
    protected long size;
    protected final long sizeThreshold;
    // the count of entries since reset and a modulus threshold
    protected long count;
    protected final long countThreshold;

    public final List<DecoratedKey> keys;
    public final List<List<ByteBuffer>> names;
    // a bitset per level that toggles when the parent changes
    public final List<IndexedBitSet> parents;
    // a boolean to record the parent flag between resets: a successful reset stores
    // the final parent flag at each level: an unsuccessful reset does not
    // TODO: this is to assist blocking, despite the fact single rows are observed
    public final boolean[] leading;
    // the absolute offsets of the observed entries
    // TODO: avoid boxing
    public final List<Long> offsets;

    public Observer(int depth, int expectedEntries, long sizeThreshold, long countThreshold)
    {
        this.size = 0;
        this.sizeThreshold = sizeThreshold;
        this.count = 0;
        this.countThreshold = countThreshold;
        this.keys = new ArrayList<DecoratedKey>(expectedEntries);
        this.names = new ArrayList<List<ByteBuffer>>(depth - 1);
        this.parents = new ArrayList<IndexedBitSet>(depth - 1);
        for (int i = 1; i < depth; i++)
        {
            this.names.add(new ArrayList<ByteBuffer>(expectedEntries));
            this.parents.add(new IndexedBitSet(expectedEntries));
        }
        this.leading = new boolean[depth - 1];
        this.offsets = new ArrayList<Long>();
    }

    /** @return The number of times the Observer has been increment()ed. */
    public long count()
    {
        return count;
    }

    /**
     * @return True if a value at the given depth should be recorded.
     */
    public abstract boolean shouldAdd(int depth, boolean last);

    /**
     * Increments the count and adds to the data size.
     */
    public void increment(long bytes)
    {
        count++;
        size += bytes;
    }

    /** Adds a key that shouldAdd requested. */
    public void add(DecoratedKey key, long position)
    {
        assert keys.isEmpty() : "TODO: Observer can only observe a row at a time";
        keys.add(key);
        offsets.add(position);
        // toggle the parents of our children, creating implicit "empty" children
        toggleFrom(1);
        size = 0;
    }

    /** Adds a name that shouldAdd requested at the particular depth. */
    public void add(int depth, ByteBuffer name, long position)
    {
        names.get(depth - 1).add(name);
        offsets.add(position);
        // increment our own index without toggling
        incrementAt(depth, false);
        // and toggle and increment our children (if we have any)
        toggleFrom(depth + 1);
        size = 0;
    }

    /** Increments the count of names and toggles the parent flag below depth. */
    private void toggleFrom(int depth)
    {
        for (; depth <= names.size(); depth++)
            // toggle the parent bit and increment
            incrementAt(depth, true);
    }

    private void incrementAt(int depth, boolean toggle)
    {
        IndexedBitSet p = parents.get(depth - 1);
        // get the current value of the parent flag at this level
        boolean current = p.index == 0 ? leading[depth - 1] : p.get(p.index - 1);
        if (current ^ toggle)
            p.set(p.index);
        p.index++;
    }

    /**
     * Resets the observer for use on a new row: true if the content was successfully
     * captured, false otherwise.
     * TODO: The annoying stateful-ness could be removed if an observer
     * observed an entire block in one go
     */
    public void reset(boolean success)
    {
        count = 0;
        size = 0;
        keys.clear();
        for (int i = 0; i < names.size(); i++)
        {
            names.get(i).clear();
            IndexedBitSet p = parents.get(i);
            if (success && p.index > 0)
                // record the new leading parent flag
                leading[i] = p.get(p.index - 1);
            p.clear();
        }
        offsets.clear();
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder("#<Observer ");
        buff.append(keys).append(' ');
        // TODO: not tostringing names
        buff.append(offsets.toString());
        return buff.append(">").toString();
    }

    /** Adds a mutable index field to OpenBitSet. */
    public static class IndexedBitSet extends OpenBitSet
    {
        public long index = 0;

        public IndexedBitSet(long expected)
        {
            super(expected);
        }

        public IndexedBitSet(long[] bits, int numWords)
        {
            super(bits, numWords);
        }

        /** Copies from that bitset to the end of this bitset. */
        public void copyFrom(IndexedBitSet that)
        {
            // TODO: could copy directly as longs given a bit of shifting
            for (int i = 0; i < that.index; i++, this.index++)
                if (that.get(i))
                    this.set(this.index);
        }

        /**
         * @return The end of a run of equal bits starting at the given index in
         * the bitset.
         */
        public long endOfRun(long from)
        {
            long end = nextSetBit(from);
            if (end == -1)
                // the remainder of the bitset is a run of unset bits
                return index;
            else if (end > from)
                // there is a run of unset bits up to the next set bit
                return end;
            // else: fall back to a loop to find the next unset bit
            while (++end < index)
                if (!fastGet(end))
                    // found the end of the set-run
                    break;
            return end;
        }

        public void clear()
        {
            this.clear(0, index);
            index = 0;
        }
        
        /** @return A LongCollection that "appends" the index to the bit storage. */
        public LongType.LongCollection asLongCollection()
        {
            // the number of longs that are active in the bitset, plus the index
            final int numWords = OpenBitSet.bits2words(index);
            return new LongType.LongCollection(numWords + 1)
            {
                public long get(int i)
                {
                    return i == numWords ? index : bits[i];
                }
            };
        }

        /** The inverse of asLongCollection. */
        public static IndexedBitSet from(long[] collection)
        {
            int numWords = collection.length - 1;
            IndexedBitSet i = new IndexedBitSet(collection, numWords);
            i.index = collection[numWords];
            return i;
        }
    }
}
