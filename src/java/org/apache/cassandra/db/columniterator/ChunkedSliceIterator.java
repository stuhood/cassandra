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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.BlockHeader;
import org.apache.cassandra.io.sstable.Chunk;
import org.apache.cassandra.io.sstable.Cursor;
import org.apache.cassandra.io.sstable.BlockHeader;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
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

    private Iterator<BlockHeader> headers;
    private FileDataInput file;
    private FileMark mark;
    private long markPosition;

    // the current chunks, flag for column consumption
    private Cursor cursor;
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
        this.key = key;
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        this.available = false;

        List<BlockHeader> headers = sstable.getPositions(key, SSTableReader.Operator.EQ);
        if (headers == null)
            return;
        System.out.println("Slicing on " + key);
        if (headers.get(0).isMetadataSet())
        {
            // apply metadata from the index
            cf = ColumnFamily.create(sstable.metadata);
            cf.delete(headers.get(0).localDeletionTime(), headers.get(0).markedForDeleteAt());

            // filter the blockheaders to only seek to the matching columns
            List<BlockHeader> filtered = null;
            AbstractType c = sstable.getColumnComparator();
            for (BlockHeader header : headers)
            {
                if (header.min() == null)
                {
                    // the row is a tombstone, and we've gotten the metadata: finished.
                    System.out.println("\ttombstone");
                    return;
                }
                try
                {
                    System.out.println("\t" + ByteBufferUtil.string(header.min()) + ", " + ByteBufferUtil.string(header.max()));
                }
                catch (Exception e)
                {
                    // pass
                }
                if (finishColumn.remaining() > 0 && c.compare(finishColumn, header.min()) < 0)
                    // the minimum column in this row is greater than the queries' max
                    continue;
                if (startColumn.remaining() > 0 && c.compare(header.max(), startColumn) < 0)
                    // the maximum column in this row is less than the queries' min
                    continue;
                if (filtered == null)
                    // lazily create the filtered list
                    filtered = new ArrayList<BlockHeader>();
                filtered.add(header);
            }
            if (filtered == null)
                // no interesting blocks in this file
                return;
            headers = filtered;
        }
        this.headers = headers.iterator();

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
        this.key = key;
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        this.available = false;
        this.headers = null;
        this.cursor = cursor;
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
        try
        {
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
        if (file == null)
            return;
        if (headers != null)
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
