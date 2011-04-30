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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.Chunk;
import org.apache.cassandra.io.sstable.Cursor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.FBUtilities;

/**
 *  A Column Iterator over SSTable
 */
public class ChunkedSliceIterator implements IColumnIterator
{
    private final SSTableReader sstable;
    private final ByteBuffer startColumn;
    private final ByteBuffer finishColumn;

    private DecoratedKey key;
    private ColumnFamily cf;

    private final FileDataInput file;
    private boolean shouldClose;
    protected final boolean isSuper;

    // the current chunks, flag for column consumption
    private final Cursor cursor;
    private boolean available;

    /**
     * An eagerly loaded iterator for a slice within an SSTable.
     * @param metadata Metadata for the CFS we are reading from
     * @param file Optional parameter that input is read from.  If null is passed, this class creates an appropriate one automatically.
     * If this class creates, it will close the underlying file when #close() is called.
     * If a caller passes a non-null argument, this class will NOT close the underlying file when the iterator is closed (i.e. the caller is responsible for closing the file)
     * In all cases the caller should explicitly #close() this iterator.
     * @param key The key the requested slice resides under, or null.
     * @param startColumn The start of the slice
     * @param finishColumn The end of the slice
     * @param reversed Results are returned in reverse order iff reversed is true.
     */
    public ChunkedSliceIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        this.sstable = sstable;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.key = key;
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        this.available = false;
        this.cursor = new Cursor(sstable.descriptor, sstable.metadata.getTypes());
        assert !reversed : "TODO: Support reversed slices";

        if (file != null)
        {
            shouldClose = false;
            this.file = file;
        }
        else
        {
            shouldClose = true;
            this.file = sstable.getFileDataInput(this.key, DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);
            if (this.file == null)
                return;
        }
        this.readSpan(); // eagerly load the chunk/key/cf
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
        try
        {
            // read all chunks for the span
            cursor.nextSpan(file);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        // validate our position
        if (key == null)
            key = sstable.partitioner.decorateKey(cursor.getVal(cursor.keyDepth()));
        else
            assert cursor.getVal(cursor.keyDepth()).equals(key.key) : "Positioned at the wrong row!";
        // read range metadata to reconstruct column family
        if (cf == null)
        {
            cf = ColumnFamily.create(sstable.metadata);
            cursor.applyMetadata(1, cf); // only one key per span
        }

        // find the window of interesting columns in this span
        if (cursor.searchAt(1, startColumn) < 0)
            // nothing gte in the span means nothing in further spans either
            return false;
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
            if (!cursor.isConsumed(1, finishColumn.remaining() == 0 ? null : finishColumn))
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

        return cursor.getColumn();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        if (shouldClose)
        {
            file.close();
            return;
        }
        // else
        // consume the remainder of this row
        assert key != null : "Closing a row that isn't open?";
        while (cursor.hasMoreSpans())
        {
            cursor.nextSpan(file);
            // confirm that we are still consuming the correct row
            assert cursor.getVal(cursor.keyDepth()).equals(key.key) :
                "Positioned at the wrong row!";
        }
    }
}
