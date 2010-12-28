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

import java.io.Closeable;
import java.io.File;
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
import org.apache.cassandra.io.Scanner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.ByteBufferUtil;


public final class SSTableScanner implements Scanner
{
    private static Logger logger = LoggerFactory.getLogger(SSTableScanner.class);

    public final SSTableReader sstable;
    protected IColumnIterator row;
    protected boolean exhausted = false;
    protected Iterator<IColumnIterator> iterator;
    protected QueryFilter filter;
    private final BufferedRandomAccessFile file;

    /**
     * @param sstable SSTable to scan.
     */
    SSTableScanner(SSTableReader sstable, int bufferSize, boolean skipCache)
    {
        this.sstable = sstable;
        try
        {
            this.file = new BufferedRandomAccessFile(new File(sstable.getFilename()), "r", bufferSize, skipCache);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * @param sstable SSTable to scan.
     * @param filter filter to use when scanning the columns
     */
    SSTableScanner(SSTableReader sstable, QueryFilter filter, int bufferSize)
    {
        this.sstable = sstable;
        try
        {
            this.file = new BufferedRandomAccessFile(sstable.getFilename(), "r", bufferSize);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        this.filter = filter;
    }

    public SSTableReader getSSTable()
    {
        return sstable;
    }

    public void close() throws IOException
    {
        file.close();
    }

    public void seekTo(DecoratedKey seekKey)
    {
        seekTo(sstable.getPosition(seekKey, SSTableReader.Operator.GE));
    }

    public void seekTo(long position)
    {
        if (position < 0)
        {
            exhausted = true;
            return;
        }
        try
        {
            file.seek(position);
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

    private class KeyScanningIterator implements Iterator<IColumnIterator>
    {
        public boolean hasNext()
        {
            try
            {
                if (row != null)
                {
                    // consume the row to position ourselves at its end
                    row.close();
                    row = null;
                }
                return !file.isEOF();
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
            try
            {
                if (filter != null)
                    return row = filter.getSSTableColumnIterator(sstable, file);
                return row = SSTableIdentityIterator.create(sstable, file, false);
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
            return getClass().getSimpleName() + "(@" + file.getFilePointer() + ")";
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "file=" + file +
               " sstable=" + sstable +
               " exhausted=" + exhausted +
               ")";
    }
}
