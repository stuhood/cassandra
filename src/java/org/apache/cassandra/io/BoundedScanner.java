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
package org.apache.cassandra.io;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

/**
 * A Scanner that only reads key in a given range (for validation compaction).
 */
public class BoundedScanner implements Scanner
{
    private final Scanner scanner;
    private final Iterator<Pair<Long, Long>> rangeIterator;
    private Pair<Long, Long> currentRange;

    public BoundedScanner(Scanner scanner, Range range)
    {
        this.scanner = scanner;
        this.rangeIterator = scanner.getSSTable().getPositionsForRanges(Collections.singletonList(range)).iterator();
        if (rangeIterator.hasNext())
        {
            currentRange = rangeIterator.next();
            seekToCurrentRange();
        }
        else
            currentRange = null;
    }

    public SSTableReader getSSTable()
    {
        return scanner.getSSTable();
    }

    private void seekToCurrentRange()
    {
        scanner.seekTo(currentRange.left);
    }

    public void seekTo(long position)
    {
        throw new AssertionError("BoundedScanner is intended for sequential access");
    }

    public void seekTo(DecoratedKey key)
    {
        seekTo(-1);
    }

    public long getFileLength()
    {
        return scanner.getFileLength();
    }

    public long getFilePointer()
    {
        return scanner.getFilePointer();
    }

    public boolean hasNext()
    {
        if (currentRange == null)
            return false;
        while (currentRange.right <= scanner.getFilePointer())
        {
            if (!rangeIterator.hasNext())
                return false;
            currentRange = rangeIterator.next();
            seekToCurrentRange();
        }
        return true;
    }

    public IColumnIterator next()
    {
        if (!hasNext())
            throw new NoSuchElementException();
        return scanner.next();
    }

    public void close() throws IOException
    {
        scanner.close();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
