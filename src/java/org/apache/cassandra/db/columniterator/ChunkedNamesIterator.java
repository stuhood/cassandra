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
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.utils.FBUtilities;

public class ChunkedNamesIterator extends SimpleAbstractColumnIterator implements IColumnIterator
{
    private static Logger logger = LoggerFactory.getLogger(ChunkedNamesIterator.class);

    private DecoratedKey key;
    private ColumnFamily cf;
    private Iterator<IColumn> iter;

    private Iterator<BlockHeader> headers;
    private FileDataInput file;
    private FileMark mark;
    private long markPosition;

    public final SortedSet<ByteBuffer> columns;
    public final SSTableReader sstable;
    public final boolean isSuper;
    public Cursor cursor;

    public ChunkedNamesIterator(SSTableReader sstable, DecoratedKey key, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.columns = columns;
        this.sstable = sstable;
        this.key = key;

        List<BlockHeader> headers = sstable.getPositions(key, SSTableReader.Operator.EQ);
        if (headers == null)
            return;
        // filter headers to decide which blocks to visit
        if (headers.get(0).isMetadataSet())
        {
            // apply metadata from the index
            cf = ColumnFamily.create(sstable.metadata);
            cf.delete(headers.get(0).localDeletionTime(), headers.get(0).markedForDeleteAt());

            if (columns.isEmpty())
                return;

            // filter the blockheaders to only seek to the matching columns
            List<BlockHeader> filtered = null;
            AbstractType c = sstable.getColumnComparator();
            for (BlockHeader header : headers)
            {
                // and decide whether we need to open the data file
                if (header.min() == null)
                    // the row is a tombstone, and we've gotten the metadata: finished.
                    return;
                if (c.compare(columns.last(), header.min()) < 0)
                    // the minimum column in this row is greater than the queries' max
                    continue;
                if (c.compare(header.max(), columns.first()) < 0)
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
        read();
    }

    public ChunkedNamesIterator(SSTableReader sstable, FileDataInput file, Cursor cursor, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.columns = columns;
        this.headers = null;
        this.sstable = sstable;
        this.file = file;
        this.cursor = cursor;
        read();
    }

    /** Called on (re)open to initialize the key and metadata. */
    private void init() throws IOException
    {
        if (cursor.isAvailable())
        {
            // new row from existing cursor: get key
            key = cursor.getKey(sstable.partitioner, key);
            readMetadata(file);
        }
        else
        {
            // empty cursor
            if (!readSpan())
                // an iterator was opened in an invalid position
                throw new IOError(new IOException("Invalid position: " + cursor + ", " + file));
        }
    }
    
    /** @return True if we opened the next valid span for this iterator. */
    private boolean readSpan() throws IOException
    {
        if (headers != null)
        {
            if (!headers.hasNext())
                return false;
            BlockHeader header = headers.next();
            if (file == null)
                // open the file for the first time
                file = sstable.getFileDataInput(header, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
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
        // consume top level range metadata at the head of the span (currently unused)
        cursor.getMetadata(0);
        // validate that we're positioned at the correct row
        key = cursor.getKey(sstable.partitioner, key);
        // load cf metadata
        readMetadata(file);
        return true;
    }

    private void readMetadata(FileDataInput file)
    {
        // read range metadata to reconstruct column family
        cf = ColumnFamily.create(sstable.metadata);
        cursor.applyMetadata(1, cf);
    }

    private void read()
    {
        try
        {
            init();
            innerRead();
            if (headers == null && file != null)
                consume();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            if (file != null)
                FileUtils.closeQuietly(file);
        }
    }

    private void innerRead() throws IOException
    {
        // match columns
        List<IColumn> matched = new ArrayList<IColumn>(columns.size());
        // TODO: wasteful: searching for every column in every span
        // is the current column valid?
        outer: for (ByteBuffer column : columns)
        {
            // -1 if we reached a sentinel, 0 for a match, 1 for a miss
            if (cursor.searchAt(1, column) > 0)
                // not found
                continue;
            // else, either sentinel or column
            byte m = cursor.peekAt(1, columns.last());
            switch (m)
            {
                case Chunk.ENTRY_NAME:
                case Chunk.ENTRY_PARENT:
                    // column available
                    matched.add(cursor.getColumn());
                    break;
                case Chunk.ENTRY_RANGE_END_NULL:
                    // sentinel type: no more columns for this parent
                    // close() will consume the sentinel
                    break outer;
                case Chunk.ENTRY_RANGE_END:
                    // span is ending on a split (in future, could mean a range delete)
                    if (!readSpan())
                        // no more matched spans
                        break outer;
                    // successfully read a new span
                    break;
                default:
                    throw new AssertionError("Bad metadata type for position: " + m);
            }
        }

        iter = matched.iterator();
    }

    private void consume() throws IOException
    {
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
                    if (!readSpan())
                        throw new IOException("Current block ends with a continuation, but no more blocks are available.");
                    // successfully read a new span
                    break;
                default:
                    throw new AssertionError("Unsupported metadata type for this level: " + m);
            }
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

    protected IColumn computeNext()
    {
        if (iter == null || !iter.hasNext())
            return endOfData();
        return iter.next();
    }
}
