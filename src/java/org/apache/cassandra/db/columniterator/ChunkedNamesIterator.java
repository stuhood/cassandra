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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.avro.*;
import org.apache.cassandra.io.sstable.Chunks;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

public class ChunkedNamesIterator extends SimpleAbstractColumnIterator implements IColumnIterator
{
    private static Logger logger = LoggerFactory.getLogger(ChunkedNamesIterator.class);

    private static final SpecificDatumReader<Chunk> dreader = new SpecificDatumReader<Chunk>();

    private DecoratedKey key;
    private ColumnFamily cf;
    private Iterator<IColumn> iter;

    public final SortedSet<ByteBuffer> columns;
    public final SSTableReader sstable;
    public final boolean isSuper;
    public final Chunks.Cursor cursor;

    public ChunkedNamesIterator(SSTableReader sstable, DecoratedKey key, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.columns = columns;
        this.sstable = sstable;
        this.key = key;
        this.cursor = new Chunks.Cursor(isSuper);

        FileDataInput file = sstable.getFileDataInput(this.key, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
        if (file == null)
            return;
        try
        {
            // TODO: avro needs to peek at the header to initialize: this _should_
            // always be cached, but...
            long keyOffset = file.tell();
            file.seek(0);
            DataFileReader<Chunk> reader = new DataFileReader<Chunk>(file, dreader);
            reader.seek(keyOffset);
            read(reader);
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

    public ChunkedNamesIterator(SSTableReader sstable, DataFileReader<Chunk> reader, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.columns = columns;
        this.sstable = sstable;
        this.cursor = new Chunks.Cursor(isSuper);

        try
        {
            read(reader);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /** @return True if we opened the next valid span for this iterator. */
    private boolean readSpan(DataFileReader<Chunk> reader)
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
            cursor.chunks[3] = reader.next();
        return true;
    }

    private void read(DataFileReader<Chunk> reader) throws IOException
    {
        // match columns
        List<IColumn> matched = new ArrayList<IColumn>(columns.size());
        // TODO: wasteful: searching for every column in every span
        while (readSpan(reader))
        {
            for (ByteBuffer column : columns)
            {
                cursor.indexes[1] = Collections.binarySearch(cursor.chunks[1].values, column, sstable.metadata.comparator);
                if (cursor.indexes[1] < 0)
                    // not found
                    continue;
                if (isSuper)
                    // find the first child of the matched column
                    cursor.seekToFirstChild(1);
                matched.add(Chunks.getColumn(cursor, sstable.metadata));
            }
        }
        iter = matched.iterator();
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
