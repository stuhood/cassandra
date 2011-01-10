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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.columniterator.ChunkedSliceIterator;
import org.apache.cassandra.io.sstable.avro.*;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;

class ChunkedIdentityIterator extends SSTableIdentityIterator
{
    private static final SpecificDatumReader<Chunk> dreader = new SpecificDatumReader<Chunk>();

    public final SSTableReader sstable;
    private DecoratedKey key;
    private ColumnFamily cf;

    protected final DataFileReader<Chunk> reader;
    protected final boolean isSuper;

    private final long startPosition;
    // not valid until closed
    private long finishPosition = -1;
    private int columnCount = -1;

    // the current chunks, flag for column consumption
    private final Chunks.Cursor cursor;
    private boolean available;

    /**
     * Used to iterate through all columns in a row.
     */
    ChunkedIdentityIterator(SSTableReader sstable, DataFileReader<Chunk> reader, boolean fromRemote)
    {
        super(sstable.metadata, sstable.partitioner, sstable.descriptor, fromRemote);
        this.sstable = sstable;
        this.reader = reader;
        this.startPosition = reader.tell();
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.cursor = new Chunks.Cursor(isSuper);
        this.available = false;
        this.readSpan(); // eagerly open
    }

    public SSTableReader getSSTable()
    {
        return sstable;
    }

    public String getPath()
    {
        return sstable.descriptor.filenameFor(Component.DATA);
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
    private boolean readSpan()
    {
        if (!cursor.hasMoreSpans())
            return false;
        cursor.reset();

        // the key chunk
        cursor.chunks[0] = reader.next();
        assert cursor.chunks[0].chunk == 0;
        if (key == null)
            key = sstable.partitioner.decorateKey(cursor.chunks[0].values.get(0));
        else
            assert cursor.chunks[0].values.get(0).equals(key.key) : "Positioned at the wrong row!";
        // the column name chunk
        cursor.chunks[1] = reader.next();
        if (cf == null)
        {
            // read range metadata to reconstruct column family
            cf = ColumnFamily.create(sstable.metadata);
            Chunks.applyMetadata(cursor.chunks[1], cf, 0); // one key per span
        }

        // TODO: read the remaining chunks lazily only if we match a name
        cursor.chunks[2] = reader.next();
        if (isSuper)
        {
            cursor.chunks[3] = reader.next();
            // find the first child of the current supercolumn
            cursor.seekToFirstChild(1);
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
            if (!cursor.isConsumed(1, cursor.chunks[1].values.size()))
                // nothing else to do
                break;
            // see if there is a valid span
            if (!readSpan())
                // the row is consumed
                return false;
        }
        available = true;
        return true;
    }

    public IColumn next()
    {
        if (!hasNext())
            throw new java.util.NoSuchElementException();
        available = false;
        
        return Chunks.getColumn(cursor, sstable.metadata);
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
            cursor.reset();
            // assuming that we've already read all chunks in the current span,
            // peek at the first 2 chunks in the next span
            cursor.chunks[0] = reader.next();
            assert cursor.chunks[0].values.get(0).equals(key.key) : "Positioned at the wrong row!";
            // the column name chunk
            cursor.chunks[1] = reader.next();

            // skip the remainder of the span
            reader.nextBlock();
            if (isSuper)
                reader.nextBlock();
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
