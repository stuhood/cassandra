/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.columniterator.SimpleAbstractColumnIterator;
import org.apache.cassandra.db.filter.AbstractColumnIterator;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.utils.SlabAllocator;
import org.apache.cassandra.utils.WrappedRunnable;

public class Memtable implements Comparable<Memtable>, IFlushable
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    private final AtomicBoolean isPendingFlush = new AtomicBoolean(false);
    private final AtomicInteger activeWriters = new AtomicInteger(0);

    private final AtomicLong currentThroughput = new AtomicLong(0);
    private final AtomicLong currentOperations = new AtomicLong(0);

    private final long creationTime;
    private final ConcurrentNavigableMap<DecoratedKey, ColumnFamily> columnFamilies = new ConcurrentSkipListMap<DecoratedKey, ColumnFamily>();
    public final ColumnFamilyStore cfs;

    private final long THRESHOLD;
    private final long THRESHOLD_COUNT;
    private SlabAllocator allocator = new SlabAllocator(DatabaseDescriptor.getOffHeapMemtables());

    public Memtable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        creationTime = System.currentTimeMillis();
        THRESHOLD = cfs.getMemtableThroughputInMB() * 1024L * 1024L;
        THRESHOLD_COUNT = (long) (cfs.getMemtableOperationsInMillions() * 1024 * 1024);
    }

    /**
     * Compares two Memtable based on creation time.
     * @param rhs Memtable to compare to.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    public int compareTo(Memtable rhs)
    {
    	long diff = creationTime - rhs.creationTime;
    	if ( diff > 0 )
    		return 1;
    	else if ( diff < 0 )
    		return -1;
    	else
    		return 0;
    }

    public long getCurrentThroughput()
    {
        return currentThroughput.get();
    }
    
    public long getCurrentOperations()
    {
        return currentOperations.get();
    }

    boolean isThresholdViolated()
    {
        return currentThroughput.get() >= this.THRESHOLD || currentOperations.get() >= this.THRESHOLD_COUNT;
    }

    boolean isPendingFlush()
    {
        return isPendingFlush.get();
    }

    boolean markPendingFlush()
    {
        return isPendingFlush.compareAndSet(false, true);
    }

    /**
     * Should only be called by ColumnFamilyStore.apply.  NOT a public API.
    */
    void put(DecoratedKey key, ColumnFamily columnFamily)
    {
        try
        {
            activeWriters.incrementAndGet();
            resolve(key, columnFamily);
        }
        finally
        {
            activeWriters.decrementAndGet();
        }
    }

    private void resolve(DecoratedKey key, ColumnFamily cf)
    {
        currentThroughput.addAndGet(cf.size());
        currentOperations.addAndGet(cf.getColumnCount());

        ColumnFamily clonedCf = columnFamilies.get(key);
        // if the row doesn't exist yet in the memtable, clone cf to our allocator.
        if (clonedCf == null)
        {
            clonedCf = cf.cloneMeShallow();
            for (IColumn column : cf.getSortedColumns())
                clonedCf.addColumn(column.localCopy(cfs, allocator));
            clonedCf = columnFamilies.putIfAbsent(key, clonedCf);
            if (clonedCf == null)
                return;
            // else there was a race and the other thread won.  fall through to updating his CF object
        }

        // we duplicate the funcationality of CF.resolve here to avoid having to either pass the Memtable in for
        // the cloning operation, or cloning the CF container as well as the Columns.  fortunately, resolve
        // is really quite simple:
        clonedCf.delete(cf);
        for (IColumn column : cf.getSortedColumns())
            clonedCf.addColumn(column.localCopy(cfs, allocator));
    }

    // for debugging
    public String contents()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : columnFamilies.entrySet())
        {
            builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        builder.append("}");
        return builder.toString();
    }


    private SSTableReader writeSortedContents() throws IOException
    {
        logger.info("Writing " + this);
        SSTableWriter writer = cfs.createFlushWriter(columnFamilies.size());

        for (Map.Entry<DecoratedKey, ColumnFamily> entry : columnFamilies.entrySet())
            writer.append(entry.getKey(), entry.getValue());

        SSTableReader ssTable = writer.closeAndOpenReader();
        logger.info(String.format("Completed flushing %s (%d bytes)",
                                  ssTable.getFilename(), new File(ssTable.getFilename()).length()));
        allocator.free();
        return ssTable;
    }

    public void flushAndSignal(final CountDownLatch latch, ExecutorService sorter, final ExecutorService writer)
    {
        writer.execute(new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                waitForWriters();
                cfs.flushLock.lock();
                try
                {
                    if (!cfs.isDropped())
                    {
                        SSTableReader sstable = writeSortedContents();
                        cfs.replaceFlushed(Memtable.this, sstable);
                    }
                }
                finally
                {
                    cfs.flushLock.unlock();
                }
                latch.countDown();
            }
        });
    }

    /*
     * Wait for all writers to be done with this memtable before flushing.
     * A busy-wait is probably alright since we'll new wait long.
     */
    private void waitForWriters()
    {
        while (activeWriters.get() > 0)
        {
            try
            {
                Thread.sleep(3);
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting on writers.", e);
            }
        }
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s bytes, %s operations)",
                             cfs.getColumnFamilyName(), hashCode(), currentThroughput, currentOperations);
    }

    /**
     * @param startWith Include data in the result from and including this key and to the end of the memtable
     * @return An iterator of entries with the data from the start key 
     */
    public Iterator<Map.Entry<DecoratedKey, ColumnFamily>> getEntryIterator(DecoratedKey startWith)
    {
        return columnFamilies.tailMap(startWith).entrySet().iterator();
    }

    public boolean isClean()
    {
        return columnFamilies.isEmpty();
    }

    public String getTableName()
    {
        return cfs.table.name;
    }

    /**
     * obtain an iterator of columns in this memtable in the specified order starting from a given column.
     */
    public static IColumnIterator getSliceIterator(final DecoratedKey key, final ColumnFamily cf, SliceQueryFilter filter, AbstractType typeComparator)
    {
        assert cf != null;
        final boolean isSuper = cf.isSuper();
        final Collection<IColumn> filteredColumns = filter.reversed ? cf.getReverseSortedColumns() : cf.getSortedColumns();

        // ok to not have subcolumnComparator since we won't be adding columns to this object
        IColumn startColumn = isSuper ? new SuperColumn(filter.start, (AbstractType)null) :  new Column(filter.start);
        Comparator<IColumn> comparator = filter.getColumnComparator(typeComparator);

        final PeekingIterator<IColumn> filteredIter = Iterators.peekingIterator(filteredColumns.iterator());
        if (!filter.reversed || filter.start.remaining() != 0)
        {
            while (filteredIter.hasNext() && comparator.compare(filteredIter.peek(), startColumn) < 0)
            {
                filteredIter.next();
            }
        }

        return new AbstractColumnIterator()
        {
            public ColumnFamily getColumnFamily()
            {
                return cf;
            }

            public DecoratedKey getKey()
            {
                return key;
            }

            public boolean hasNext()
            {
                return filteredIter.hasNext();
            }

            public IColumn next()
            {
                return filteredIter.next();                
            }
        };
    }

    public static IColumnIterator getNamesIterator(final DecoratedKey key, final ColumnFamily cf, final NamesQueryFilter filter)
    {
        assert cf != null;
        final boolean isStandard = !cf.isSuper();

        return new SimpleAbstractColumnIterator()
        {
            private Iterator<ByteBuffer> iter = filter.columns.iterator();

            public ColumnFamily getColumnFamily()
            {
                return cf;
            }

            public DecoratedKey getKey()
            {
                return key;
            }

            protected IColumn computeNext()
            {
                while (iter.hasNext())
                {
                    ByteBuffer current = iter.next();
                    IColumn column = cf.getColumn(current);
                    if (column != null)
                        // clone supercolumns so caller can freely removeDeleted or otherwise mutate it
                        return isStandard ? column : ((SuperColumn)column).cloneMe();
                }
                return endOfData();
            }
        };
    }

    public ColumnFamily getColumnFamily(DecoratedKey key)
    {
        return columnFamilies.get(key);
    }

    void clearUnsafe()
    {
        columnFamilies.clear();
    }

    public boolean isExpired()
    {
        return System.currentTimeMillis() > creationTime + cfs.getMemtableFlushAfterMins() * 60 * 1000L;
    }
}
