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
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.BoundedBitSet;

/**
 * An object that observes and records tuples when thresholds are met.
 */
final class Observer
{
    // list of observed keys
    public final List<ByteBuffer> keys;
    // a sparse list of observed column names per level
    public final List<ByteBuffer> names;
    public final BoundedBitSet nameparents;
    // sparse lists of metadata using the same parent-bit encoding as names
    public final List<Long> markedForDeleteAt;
    public final List<Long> localDeletionTime;
    public final BoundedBitSet metaparent;
    // the absolute offsets of the observed entries (currently boxed: yuck)
    public final List<Long> offsets;

    // booleans to record the parent flag between resets: a successful reset stores
    // the final parent flag: an unsuccessful reset does not
    // (this is to assist blocking, since we observe/reset at the row level: see reset(boolean))
    public boolean nameslead;
    public boolean metalead;

    public Observer(int expectedEntries)
    {
        this.keys = new ArrayList<ByteBuffer>(expectedEntries);
        this.markedForDeleteAt = new ArrayList<Long>(expectedEntries);
        this.localDeletionTime = new ArrayList<Long>(expectedEntries);
        this.metaparent = new BoundedBitSet(expectedEntries);
        this.names = new ArrayList<ByteBuffer>(expectedEntries);
        this.nameparents = new BoundedBitSet(expectedEntries);
        this.nameslead = false;
        this.offsets = new ArrayList<Long>();
    }

    /**
     * Adds a row key.
     */
    public void add(DecoratedKey key, long position)
    {
        assert keys.isEmpty() : "Observer can only observe a row at a time";
        keys.add(key.key);
        offsets.add(position);
        // toggle the parent flag for metadata
        increment(metaparent, metalead, true);
        // toggle the parents of child names, creating implicit "empty" children
        increment(nameparents, nameslead, true);
    }

    /**
     * Adds metadata for the children of the current row key.
     */
    public void add(long markedForDeleteAt, int localDeletionTime)
    {
        assert keys.size() == 1 : "A key is not being observed.";
        this.markedForDeleteAt.add(markedForDeleteAt);
        this.localDeletionTime.add((long)localDeletionTime);
        // increment the metadata index without toggling
        increment(metaparent, metalead, false);
    }

    /** Adds a name that shouldAdd requested at the particular depth. */
    public void add(ByteBuffer name, long position)
    {
        names.add(name);
        offsets.add(position);
        // increment our own index without toggling
        increment(nameparents, nameslead, false);
    }

    private void increment(BoundedBitSet p, boolean leading, boolean toggle)
    {
        // get the current value of the parent flag at this level
        boolean current = p.index() == 0 ? leading : p.get(p.index() - 1);
        if (current ^ toggle)
            p.set();
        else
            p.unset();
    }

    /**
     * Resets the observer for use on a new row: true if the content was successfully
     * captured, false otherwise.
     */
    public void reset(boolean success)
    {
        keys.clear();
        markedForDeleteAt.clear();
        localDeletionTime.clear();
        metalead = leadAndClear(metaparent, metalead, success);
        names.clear();
        nameslead = leadAndClear(nameparents, nameslead, success);
        offsets.clear();
    }

    /**
     * The annoying stateful-ness of the "lead" booleans could be removed if an
     * observer observed an entire block in one go.
     */
    public boolean leadAndClear(BoundedBitSet p, boolean lead, boolean success)
    {
        if (success)
            // record the new nameslead parent flag
            lead = p.get(p.index() - 1);
        p.clear();
        return lead;
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder("#<Observer ");
        buff.append(offsets.toString());
        return buff.append(">").toString();
    }
}
