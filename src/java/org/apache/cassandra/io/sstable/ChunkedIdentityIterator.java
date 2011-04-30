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
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.columniterator.ChunkedSliceIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Chunk;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;

class ChunkedIdentityIterator extends SSTableIdentityIterator
{
    private DecoratedKey key;
    private ColumnFamily cf;

    protected final BufferedRandomAccessFile file;

    private final long startPosition;
    // not valid until closed
    private long finishPosition = -1;
    private int columnCount = -1;

    // the current chunks, flag for column consumption
    private final Cursor cursor;
    private boolean available;

    /**
     * Used to iterate through all columns in a row.
     */
    ChunkedIdentityIterator(CFMetaData cfm, IPartitioner partitioner, Descriptor desc, BufferedRandomAccessFile file, boolean fromRemote) throws IOException
    {
        super(cfm, partitioner, desc, fromRemote);
        this.file = file;
        this.startPosition = file.getFilePointer();
        this.cursor = new Cursor(desc, cfm.getTypes(), fromRemote);
        this.available = false;
        this.readSpan(); // eagerly open
    }

    public String getPath()
    {
        return desc.filenameFor(Component.DATA);
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    public long getDataSize()
    {
        assert finishPosition != -1 : "Row has not been consumed: must call close.";
        return finishPosition - startPosition;
    }

    /** @return True if we opened the next valid span for this iterator. */
    private boolean readSpan() throws IOException
    {
        if (!cursor.hasMoreSpans())
            return false;
        // read all chunks for the span
        cursor.nextSpan(file);

        // validate our position
        if (key == null)
            key = partitioner.decorateKey(cursor.getVal(cursor.keyDepth()));
        else
            assert cursor.getVal(cursor.keyDepth()).equals(key.key) : "Positioned at the wrong row!";
        // read range metadata to reconstruct column family
        if (cf == null)
        {
            cf = ColumnFamily.create(cfm);
            cursor.applyMetadata(1, cf); // only one key per span
        }
        return true;
    }

    public boolean hasNext()
    {
        if (available)
            // column available
            return true;
        while(true)
        {
            // is the current column valid?
            if (!cursor.isConsumed(1, null))
                // nothing else to do
                break;
            try
            {
                // see if there is a valid span
                if (!readSpan())
                    // the row is consumed
                    return false;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
        available = true;
        return true;
    }

    public IColumn next()
    {
        if (!hasNext())
            throw new java.util.NoSuchElementException();
        available = false;
        return cursor.getColumn();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public final void close() throws IOException
    {
        // consume the remainder of this row
        assert key != null : "Closing a row that isn't open?";
        while (cursor.hasMoreSpans())
        {
            cursor.nextSpan(file);
            // confirm that we are still consuming the correct row
            assert cursor.getVal(cursor.keyDepth()).equals(key.key) :
                "Positioned at the wrong row!";
        }
        finishPosition = file.getFilePointer();
        // TODO: would need to force decompression to get accurate count
        columnCount = 1;
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return getKey().compareTo(o.getKey());
    }

    public void echoData(DataOutput out) throws IOException
    {
        // FIXME: we should modify the uncontested row optimization into an uncontested
        // span optimization: even juicier
        throw new RuntimeException("FIXME: not implemented");
    }

    public int getColumnCount()
    {
        assert columnCount != -1 : "Row has not been consumed: must call close.";
        return columnCount;
    }

    public void reset()
    {
        try
        {
            file.seek(startPosition);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finishPosition = -1;
        columnCount = -1;
    }
}
