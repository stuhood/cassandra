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


import java.io.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.dht.IPartitioner;

public abstract class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, IColumnIterator
{
    protected final CFMetaData cfm;
    protected final IPartitioner partitioner;
    protected final Descriptor desc;
    protected final boolean fromRemote;
    // Used by lazilyCompactedRow, so that we see the same things when deserializing the first and second time
    protected final int expireBefore;

    SSTableIdentityIterator(CFMetaData cfm, IPartitioner partitioner, Descriptor desc, boolean fromRemote)
    {
        this.cfm = cfm;
        this.partitioner = partitioner;
        this.desc = desc;
        this.fromRemote = fromRemote;
        this.expireBefore = (int)(System.currentTimeMillis() / 1000);
    }

    /**
     * Construct an SSTableIdentityIterator for an sstable data file arriving
     * from a remote node, and which might be missing components. This implementation
     * also forces deserialization of the data to check for corruption.
     */
    public static SSTableIdentityIterator create(CFMetaData cfm, IPartitioner partitioner, Descriptor desc, DataInput input, boolean deserializeRowHeader) throws IOException
    {
        if (desc.isRowIndexed)
            return new RowIndexedIdentityIterator(cfm, partitioner, desc, input, deserializeRowHeader, true);
        throw new RuntimeException("FIXME");
    }

    /**
     * Open an SSTI for a local file which we "own", and which has all its components.
     */
    public static SSTableIdentityIterator create(SSTableReader sstable, DataInput input, boolean deserializeRowHeader) throws IOException
    {
        if (sstable.descriptor.isRowIndexed)
            return new RowIndexedIdentityIterator(sstable.metadata, sstable.partitioner, sstable.descriptor, input, deserializeRowHeader, false);
        throw new RuntimeException("FIXME");
    }

    public abstract String getPath();

    public abstract DecoratedKey getKey();

    /** @return An empty column family representing the metadata for the row. */
    public abstract ColumnFamily getColumnFamily();

    /** Copy the row content byte-for-byte to the given output.  */
    public abstract void echoData(DataOutput out) throws IOException;

    /**
     * @return True if the full content of the row could be buffered into a buffer
     * of the given size: valid before the iterator has been consumed.
     */
    public abstract boolean isBufferable(long limitBytes);

    /**
     * @return The width of the row: only valid after the iterator has been consumed.
     */
    public abstract long getDataSize();

    /**
     * @return The count of top-level columns in the row: only valid after the iterator
     * has been consumed.
     */
    public abstract int getColumnCount();

    /**
     * @return An estimate of the number of columns in the row: will not reposition the iterator.
     */
    public abstract int getEstimatedColumnCount();

    /** Reset the iterator to before the first column in the row. */
    public abstract void reset();

    public int compareTo(SSTableIdentityIterator o)
    {
        return getKey().compareTo(o.getKey());
    }

    @Override
    public final void remove()
    {
        throw new UnsupportedOperationException();
    }
}
