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


import java.io.DataInput;
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
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.BytesReadTracker;

class ChunkedIdentityIterator extends SSTableIdentityIterator
{
    private DecoratedKey key;
    private ColumnFamily cf;

    private BytesReadTracker input; // tracks bytes read

    // not valid until closed
    private long dataSize = -1;
    private int columnCount = -1;

    // the current chunks, flag for column consumption
    private Cursor cursor;
    private boolean available;

    // a clone of the initial cursor and input, if the input supports reset()
    private final Cursor cursorForReset;
    private final RandomAccessReader inputForReset;

    /**
     * Used to iterate through all columns in a row.
     */
    ChunkedIdentityIterator(CFMetaData cfm, IPartitioner partitioner, Descriptor desc, DataInput input, Cursor cursor, boolean fromRemote) throws IOException
    {
        super(cfm, partitioner, desc, fromRemote);

        // if the file is resetable, clone the cursor and hold onto a handle
        // FIXME: could do a shallow copy instead if we know the row is contained entirely within this chunk
        cursorForReset = input instanceof RandomAccessReader ? new Cursor(cursor).reset(cursor) : null;
        inputForReset = input instanceof RandomAccessReader ? (RandomAccessReader)input : null;
        this.input = new BytesReadTracker(input);
        this.cursor = cursor;
        this.available = false;
        this.init();
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
        assert dataSize != -1 : "Row has not been consumed: must call close.";
        return dataSize;
    }

    public boolean isBufferable(long limitBytes)
    {
        // without consuming all of the blocks of the row, it is impossible to tell
        // the actual width: request that our content be iterated
        return false;
    }

    /** Called on (re)open to initialize the key and metadata. */
    private void init()
    {
        if (cursor.isAvailable())
            // new row from existing cursor: get key
            key = cursor.getKey(partitioner, key);
        else
            // empty cursor
            readSpan();
        this.hasNext(); // eagerly load the chunk/key/cf
    }
    
    /** @return True if we opened the next valid span for this iterator. */
    private void readSpan()
    {
        try
        {
            // read all chunks for the span
            cursor.nextSpan(input);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        // consume top level range metadata at the head of the span (currently unused)
        cursor.getMetadata(0);
        // validate that we're positioned at the correct row
        key = cursor.getKey(partitioner, key);
    }

    public boolean hasNext()
    {
        if (available)
            // column available
            return true;
        outer: while(true)
        {
            // is the current column valid?
            byte m = cursor.peekAt(1, null);
            switch (m)
            {
                case Chunk.ENTRY_RANGE_BEGIN:
                case Chunk.ENTRY_RANGE_BEGIN_NULL:
                    // read range metadata to reconstruct column family
                    if (cf == null)
                        cf = ColumnFamily.create(cfm);
                    cursor.applyMetadata(1, cf);
                    break;
                case Chunk.ENTRY_NAME:
                case Chunk.ENTRY_PARENT:
                    // column available
                    break outer;
                case Chunk.ENTRY_RANGE_END_NULL:
                    // sentinel type: no more columns for this parent
                    // close() will consume the sentinel
                    return false;
                case Chunk.ENTRY_RANGE_END:
                    // span is ending on a split (in future, could mean a range delete)
                    readSpan();
                    // successfully read a new span
                    break;
                default:
                    throw new AssertionError("Unsupported metadata type for this level: " + m);
            }
        }
        available = true;
        columnCount--;
        return true;
    }

    public IColumn next()
    {
        if (!hasNext())
            throw new java.util.NoSuchElementException();
        available = false;
        return cursor.getColumn();
    }

    public void echoData(DataOutput out) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public final void close() throws IOException
    {
        // consume the remainder of this row
        outer: while (true)
        {
            byte m = cursor.skipAllAt(1);
            // TODO: would need to force decompression to get accurate column count for
            // unconsumed trailing blocks
            columnCount--;
            switch (m)
            {
                case Chunk.ENTRY_RANGE_END_NULL:
                    // sentinel types: no more columns for this parent
                    cursor.getMetadata(1);
                    break outer;
                case Chunk.ENTRY_RANGE_END:
                    // span is ending on a split (in future, could mean a range delete)
                    readSpan();
                    // successfully read a new span
                    break;
                default:
                    throw new AssertionError("Unsupported metadata type for this level: " + m);
            }
        }
        // finalize these now that the row is consumed
        columnCount = -(columnCount + 1);
        dataSize = input.getBytesRead();
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return getKey().compareTo(o.getKey());
    }

    public int getColumnCount()
    {
        assert columnCount >= 0 : "Row has not been consumed: must call close.";
        return columnCount;
    }

    public int getEstimatedColumnCount()
    {
        // FIXME: estimate based on our content size in the open block
        return 32;
    }

    public void reset()
    {
        if (cursorForReset == null)
            throw new UnsupportedOperationException("Only supported on seekable files");

        cursor.reset(cursorForReset);
        try
        {
            inputForReset.seek(inputForReset.getFilePointer() - input.getBytesRead());
            input.reset(0);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        // reinitialize the cursor
        this.available = false;
        this.init();
    }
}
