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

    public ChunkedNamesIterator(SSTableReader sstable, FileDataInput file, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.isSuper = sstable.metadata.cfType == ColumnFamilyType.Super;
        this.columns = columns;
        this.sstable = sstable;
        this.cursor = new Cursor(sstable.descriptor, sstable.metadata.getTypes());

        try
        {
            read(file);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /** @return True if we opened the next valid span for this iterator. */
    private boolean readSpan(FileDataInput file) throws IOException
    {
        if (!cursor.hasMoreSpans())
            return false;
        // read all chunks for the span
        cursor.nextSpan(file);

        // validate our position
        if (key == null)
            key = sstable.partitioner.decorateKey(cursor.getVal(cursor.keyDepth()));
        else
            assert cursor.getVal(cursor.keyDepth()).equals(key.key) : "Positioned at the wrong row!";
        // read range metadata to reconstruct column family
        if (cf == null)
        {
            cf = ColumnFamily.create(sstable.metadata);
            cursor.applyMetadata(1, cf); // only one key per span at the moment
        }
        return true;
    }

    private void read(FileDataInput file) throws IOException
    {
        // match columns
        List<IColumn> matched = new ArrayList<IColumn>(columns.size());
        // TODO: wasteful: searching for every column in every span
        while (readSpan(file))
        {
            for (ByteBuffer column : columns)
            {
                if (cursor.searchAt(1, column) != 0)
                    // not found
                    continue;
                matched.add(cursor.getColumn());
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
