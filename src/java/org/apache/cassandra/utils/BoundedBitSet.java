package org.apache.cassandra.utils;
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

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.obs.OpenBitSet;

/** Adds a mutable index field to OpenBitSet. */
public class BoundedBitSet extends OpenBitSet implements Cloneable
{
    private long index = 0;

    public BoundedBitSet(long expected)
    {
        super(expected);
    }

    public BoundedBitSet(long[] bits, int numWords)
    {
        super(bits, numWords);
    }

    /** Copies from that bitset to the end of this bitset. */
    public void copyFrom(BoundedBitSet that)
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

    /** The mutable bound stored with this bitset. */
    public long index()
    {
        return index;
    }

    /** Set the bit at the current index, and increment the index. */
    public BoundedBitSet set()
    {
        this.set(index++);
        return this;
    }

    /** Unset the bit at the current index, and Increment the index. */
    public BoundedBitSet unset()
    {
        this.clear(index++);
        return this;
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
        final int numWords = Math.min(bits.length, OpenBitSet.bits2words(index));
        return new LongType.LongCollection(numWords + 1)
        {
            public long get(int i)
            {
                return i == numWords ? index : bits[i];
            }
        };
    }

    /** The inverse of asLongCollection. */
    public static BoundedBitSet from(long[] collection)
    {
        int numWords = collection.length - 1;
        long index = collection[numWords];
        collection[numWords] = 0;
        BoundedBitSet i = new BoundedBitSet(collection, numWords);
        i.index = index;
        return i;
    }
}
