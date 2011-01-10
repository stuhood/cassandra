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


import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Filter;

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
     * Construct an SSTableIdentityIterator for an sstable data file which has arrived
     * from a remote node, and which might be missing components. This implementation
     * also forces deserialization of the data to check for corruption.
     */
    public static SSTableIdentityIterator create(CFMetaData cfm, IPartitioner partitioner, Descriptor desc, BufferedRandomAccessFile file, boolean deserializeRowHeader) throws IOException
    {
        if (desc.isRowIndexed)
            return new RowIndexedIdentityIterator(cfm, partitioner, desc, file, deserializeRowHeader, true);
        return new ChunkedIdentityIterator(cfm, partitioner, desc, file, true);
    }

    /**
     * Open an SSTI for a local file which we "own", and which has all its components.
     */
    public static SSTableIdentityIterator create(SSTableReader sstable, BufferedRandomAccessFile file, boolean deserializeRowHeader) throws IOException
    {
        if (sstable.descriptor.isRowIndexed)
            return new RowIndexedIdentityIterator(sstable.metadata, sstable.partitioner, sstable.descriptor, file, deserializeRowHeader, false);
        return new ChunkedIdentityIterator(sstable.metadata, sstable.partitioner, sstable.descriptor, file, false);
    }

    public abstract String getPath();

    public abstract DecoratedKey getKey();

    /** @return An empty column family representing the metadata for the row. */
    public abstract ColumnFamily getColumnFamily();

    public abstract void remove();

    /** Copy the row content byte-for-byte to the given output.  */
    public abstract void echoData(DataOutput out) throws IOException;

    /**
     * @return The width of the row: only valid after the iterator has been consumed.
     */
    public abstract long getDataSize();

    /**
     * @return The count of top-level columns in the row: only valid after the iterator
     * has been consumed.
     */
    public abstract int getColumnCount();

    /** Reset the iterator to before the first column in the row. */
    public abstract void reset();
}
