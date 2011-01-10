package org.apache.cassandra.db.columniterator;
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


import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.avro.*;
import org.apache.cassandra.io.sstable.Chunks;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.FBUtilities;

/**
 *  A Column Iterator over SSTable
 */
public class ChunkedSliceIterator implements IColumnIterator
{
    private static final SpecificDatumReader<Chunk> dreader = new SpecificDatumReader<Chunk>();

    private final SSTableReader sstable;
    private final ByteBuffer startColumn;
    private final ByteBuffer finishColumn;

    private DecoratedKey key;
    private ColumnFamily cf;

    private boolean shouldClose;
    private DataFileReader<Chunk> reader;
    protected final boolean isSuper;

    // the current chunks, flag for column consumption
    private final Chunks.Cursor cursor;
    private boolean available;
    // and the right bound of valid columns in the chunk
    private int right;

    public ChunkedSliceIterator(SSTableReader sstable, DecoratedKey key, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        this(sstable, null, key, startColumn, finishColumn, reversed);
    }

    public ChunkedSliceIterator(SSTableReader sstable, DataFileReader<Chunk> reader, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        this(sstable, reader, null, startColumn, finishColumn, reversed);
    }

    /**
     * An eagerly loaded iterator for a slice within an SSTable.
     * @param metadata Metadata for the CFS we are reading from
     * @param reader Optional parameter that input is read from.  If null is passed, this class creates an appropriate one automatically.
     * If this class creates, it will close the underlying file when #close() is called.
     * If a caller passes a non-null argument, this class will NOT close the underlying file when the iterator is closed (i.e. the caller is responsible for closing the file)
     * In all cases the caller should explicitly #close() this iterator.
     * @param key The key the requested slice resides under, or null.
     * @param startColumn The start of the slice
     * @param finishColumn The end of the slice
     * @param reversed Results are returned in reverse order iff reversed is true.
     */
    public ChunkedSliceIterator(SSTableReader sstable, DataFileReader<Chunk> reader, DecoratedKey key, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        this.sstable = sstable;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.key = key;
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        this.available = false;
        this.cursor = new Chunks.Cursor(isSuper);
        assert !reversed : "TODO: Support reversed slices";

        if (reader != null)
        {
            shouldClose = false;
            this.reader = reader;
            this.readSpan(); // eagerly load the chunk/key/cf
            return;
        }
        shouldClose = true;
        FileDataInput file = sstable.getFileDataInput(this.key, DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);
        if (file == null)
            return;
        try
        {
            // FIXME: avro needs to peek at the header to initialize: this _should_
            // always be cached, but...
            long keyOffset = file.tell();
            file.seek(0);
            this.reader = new DataFileReader<Chunk>(file, dreader);
            this.reader.seek(keyOffset);
            this.readSpan(); // eagerly load the chunk/key/cf
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
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

        // determine the left bound in the span using the name chunk
        cursor.searchAt(1, startColumn, sstable.metadata.comparator);

        // ...and the right bound
        if (cursor.indexes[1] >= cursor.chunks[1].values.size())
            // no matches in this chunk
            right = -1;
        else if (finishColumn.remaining() == 0)
            // unbounded to the right
            right = cursor.chunks[1].values.size();
        else
        {
            // search for right bound
            // TODO: bound binsearch by the left index
            right = cursor.binSearch(1, finishColumn, sstable.metadata.comparator);
            // right == 1 + last_interesting_name
            if (right < 0)
                right = -right - 1; // name at insertion position
            else
                right++;
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
            if (!cursor.isConsumed(1, right))
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

    public void close() throws IOException
    {
        if (shouldClose)
        {
            reader.close();
            return;
        }
        // else
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
    }
}
