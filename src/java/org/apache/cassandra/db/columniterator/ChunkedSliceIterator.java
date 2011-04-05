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
import org.apache.cassandra.io.sstable.BlockHeader;
import org.apache.cassandra.io.sstable.Chunk;
import org.apache.cassandra.io.sstable.Cursor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A Column Iterator over SSTable
 * In all cases the caller should explicitly #close() this iterator.
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
     * @param key The key the requested slice resides under.
     * @param startColumn The start of the slice
     * @param finishColumn The end of the slice
     * @param reversed Results are returned in reverse order iff reversed is true.
     */
    public ChunkedSliceIterator(SSTableReader sstable, DecoratedKey key, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        assert !reversed : "TODO: Support reversed slices";
        this.sstable = sstable;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.key = key;
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        this.available = false;
        BlockHeader header = sstable.getPosition(key, SSTableReader.Operator.EQ);
        if (header == null)
        {
            this.cursor = null;
            this.file = null;
            return;
        }
        // we opened this file handle: close it when finished
        shouldClose = true;
        this.file = sstable.getFileDataInput(header, DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);
        this.cursor = new Cursor(sstable.descriptor, sstable.metadata.getTypes());
        this.init();
        this.hasNext(); // eagerly load the chunk/key/cf
    }

    /**
     * An eagerly loaded iterator for a slice within an SSTable.
     * @param metadata Metadata for the CFS we are reading from
     * @param file A file which will NOT be closed when the iterator is closed (i.e. the caller is responsible for closing the file)
     * @param cursor A cursor which might be empty, or contain a fragment of
     * @param startColumn The start of the slice
     * @param finishColumn The end of the slice
     * @param reversed Results are returned in reverse order iff reversed is true.
     */
    public ChunkedSliceIterator(SSTableReader sstable, FileDataInput file, Cursor cursor, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        assert !reversed : "TODO: Support reversed slices";
        this.sstable = sstable;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.key = key;
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        this.available = false;
        this.cursor = cursor;
        // we did not open this file handle: don't close it
        this.shouldClose = false; 
        this.file = file;
        this.init();
        this.hasNext(); // eagerly load the chunk/key/cf
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    /** Called on (re)open to initialize the key and metadata. */
    private void init()
    {
        if (cursor.isAvailable())
            // new row from existing cursor: get key
            key = cursor.getKey(sstable.partitioner, key);
        else
        {
            // empty cursor
            if (!readSpan())
                // an iterator was opened in an invalid position
                throw new IOError(new IOException("Invalid position: " + cursor + ", " + file));
        }
    }

    /** @return True if we opened the next valid span for this iterator. */
    private boolean readSpan()
    {
        if (!cursor.hasMoreSpans())
            return false;
        try
        {
            /* FIXME: specific to 2319
            if (headers != null)
            {
                if (!headers.hasNext())
                    return false;
                BlockHeader header = headers.next();
                if (file == null)
                    // open the file for the first time
                    file = sstable.getFileDataInput(header, DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);
                else
                {
                    // seek to the next header
                    long absPosition = file.bytesPastMark(mark) + markPosition;
                    FileUtils.skipBytesFully(file, header.position() - absPosition);
                }
                mark = file.mark();
                markPosition = header.position();
            }
            */
            // read all chunks for the span
            cursor.nextSpan(file);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        // consume top level range metadata at the head of the span (currently unused)
        cursor.getMetadata(0);
        // validate that we're positioned at the correct row
        key = cursor.getKey(sstable.partitioner, key);
        return true;
    }

    public boolean hasNext()
    {
        if (available)
            // column available
            return true;
        if (cursor == null)
            return false;
        outer: while(true)
        {
            // is the current column valid?
            byte m = cursor.peekAt(1, finishColumn.remaining() == 0 ? null : finishColumn);
            switch (m)
            {
                case Chunk.ENTRY_RANGE_BEGIN:
                case Chunk.ENTRY_RANGE_BEGIN_NULL:
                    // read range metadata to reconstruct column family
                    if (cf == null)
                        cf = ColumnFamily.create(sstable.metadata);
                    cursor.applyMetadata(1, cf);
                    // find the window of interesting columns in this span
                    if (cursor.searchAt(1, startColumn) < 0)
                        // nothing gte in the span means nothing in further spans either
                        return false;
                    // read next entry
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
                    // see if there is another valid span
                    if (!readSpan())
                        // the row is consumed
                        return false;
                    // successfully read a new span
                    break;
                default:
                    throw new AssertionError("Unsupported metadata type for this level: " + m);
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

    public void close() throws IOException
    {
        if (shouldClose)
        {
            file.close();
            return;
        }
        // consume the remainder of this row
        outer: while (true)
        {
            byte m = cursor.skipAllAt(1);
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
    }
}
