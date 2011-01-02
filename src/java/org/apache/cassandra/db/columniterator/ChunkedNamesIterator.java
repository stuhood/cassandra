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
import org.apache.cassandra.io.sstable.Chunk;
import org.apache.cassandra.io.sstable.Cursor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

public class ChunkedNamesIterator extends SimpleAbstractColumnIterator implements IColumnIterator
{
    private static Logger logger = LoggerFactory.getLogger(ChunkedNamesIterator.class);

    private DecoratedKey key;
    private ColumnFamily cf;
    private Iterator<IColumn> iter;

    public final SortedSet<ByteBuffer> columns;
    public final SSTableReader sstable;
    public final boolean isSuper;
    public final Cursor cursor;

    public ChunkedNamesIterator(SSTableReader sstable, DecoratedKey key, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.columns = columns;
        this.sstable = sstable;
        this.key = key;
        this.cursor = new Cursor(sstable.descriptor, sstable.metadata.getTypes());

        FileDataInput file = sstable.getFileDataInput(this.key, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
        if (file == null)
            return;
        try
        {
            read(file);
        }
        catch (IOException ioe)
        {
            throw new IOError(ioe);
        }
        finally
        {
            FileUtils.closeQuietly(file);
        }
    }

    public ChunkedNamesIterator(SSTableReader sstable, FileDataInput file, Cursor cursor, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.columns = columns;
        this.sstable = sstable;
        this.cursor = cursor;

        try
        {
            read(file);
            consume(file);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /** Called on (re)open to initialize the key and metadata. */
    private void init(FileDataInput file) throws IOException
    {
        if (cursor.isAvailable())
        {
            // new row from existing cursor: get key
            key = cursor.getKey(sstable.partitioner, key);
            readMetadata(file);
        }
        else
            // empty cursor
            readSpan(file);
    }
    
    /** @return True if we opened the next valid span for this iterator. */
    private void readSpan(FileDataInput file) throws IOException
    {
        // read all chunks for the span
        cursor.nextSpan(file);
        // consume top level range metadata at the head of the span (currently unused)
        cursor.getMetadata(0);
        // validate that we're positioned at the correct row
        key = cursor.getKey(sstable.partitioner, key);
        // load cf metadata
        readMetadata(file);
    }

    private void readMetadata(FileDataInput file)
    {
        // read range metadata to reconstruct column family
        cf = ColumnFamily.create(sstable.metadata);
        cursor.applyMetadata(1, cf);
    }

    private void read(FileDataInput file) throws IOException
    {
        init(file);
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
                    readSpan(file);
                    // successfully read a new span
                    break;
                default:
                    throw new AssertionError("Bad metadata type for position: " + m);
            }
        }

        iter = matched.iterator();
    }

    private void consume(FileDataInput file) throws IOException
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
                    readSpan(file);
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
