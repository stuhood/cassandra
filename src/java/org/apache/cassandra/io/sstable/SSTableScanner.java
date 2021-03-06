/**
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
 */

package org.apache.cassandra.io.sstable;

import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;

public class SSTableScanner implements CloseableIterator<IColumnIterator>
{
    private static Logger logger = LoggerFactory.getLogger(SSTableScanner.class);

    protected final RandomAccessReader file;
    public final SSTableReader sstable;
    private IColumnIterator row;
    protected boolean exhausted = false;
    protected Iterator<IColumnIterator> iterator;
    private QueryFilter filter;

    private final Cursor cursor;

    /**
     * @param sstable SSTable to scan.
     */
    SSTableScanner(SSTableReader sstable, int bufferSize, boolean skipCache)
    {
        try
        {
            this.file = sstable.openDataReader(bufferSize, skipCache);
            this.cursor = new Cursor(sstable.descriptor, sstable.metadata.getTypes());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        this.sstable = sstable;
    }

    /**
     * @param sstable SSTable to scan.
     * @param filter filter to use when scanning the columns
     */
    SSTableScanner(SSTableReader sstable, QueryFilter filter, int bufferSize)
    {
        try
        {
            this.file = sstable.openDataReader(bufferSize, false);
            this.cursor = new Cursor(sstable.descriptor, sstable.metadata.getTypes());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        this.sstable = sstable;
        this.filter = filter;
    }

    public void close() throws IOException
    {
        file.close();
    }

    public void seekTo(DecoratedKey seekKey)
    {
        seekTo(sstable.getPosition(seekKey, SSTableReader.Operator.GE));
    }

    public void seekTo(BlockHeader header)
    {
        if (header == null)
        {
            exhausted = true;
            return;
        }
        seekUnsafe(header.position());
    }

    private void seekUnsafe(long position)
    {
        try
        {
            if (row != null)
                row.close();
            if (position < 0)
            {
                exhausted = true;
                return;
            }
            file.seek(position);
            cursor.clear();
            row = null;
        }
        catch (IOException e)
        {
            throw new RuntimeException("corrupt sstable", e);
        }
    }

    public long getFileLength()
    {
        try
        {
            return file.length();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public long getFilePointer()
    {
        return file.getFilePointer();
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new IColumnIterator[0]).iterator() : new KeyScanningIterator();
        return iterator.hasNext();
    }

    public IColumnIterator next()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new IColumnIterator[0]).iterator() : new KeyScanningIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    protected class KeyScanningIterator implements Iterator<IColumnIterator>
    {
        boolean prepared = false;
        public boolean hasNext()
        {
            if (prepared)
                return true;
            try
            {
                if (row != null)
                {
                    System.out.println("Closing row with " + cursor);
                    // consume the row to position ourselves at its end
                    row.close();
                    row = null;
                }
                return prepared = (!file.isEOF() || cursor.isAvailable());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public IColumnIterator next()
        {
            // hasNext will handle consuming the current row if it is still open
            if (!hasNext()) throw new NoSuchElementException();
            prepared = false;
            try
            {
                System.out.println("Opening new row +++++++++++++++++++++++++++++++");
                if (filter != null)
                    return row = filter.getSSTableColumnIterator(sstable, file, cursor);
                return row = SSTableIdentityIterator.create(sstable, file, cursor, false);
            }
            catch (IOException e)
            {
                throw new RuntimeException(SSTableScanner.this + " failed to provide next columns from " + this, e);
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "(row=:" + row + ")";
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
               "file=" + file +
               " sstable=" + sstable +
               " exhausted=" + exhausted +
               ")";
    }
}
