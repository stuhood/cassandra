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

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.AbstractCompactedRow;
import org.apache.cassandra.io.CompactionIterator;
import org.apache.cassandra.io.ICompactionInfo;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * A singleton which manages a private executor of ongoing compactions. A readwrite lock
 * controls whether compactions can proceed: an external consumer can completely stop
 * compactions by acquiring the write half of the lock via getCompactionLock().
 *
 * Scheduling for compaction is accomplished by maintaining a synchronized set of sstables
 * that are being compacted: new scheduling attempts will ignore currently compacting
 * sstables.
 */
public class CompactionManager implements CompactionManagerMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    public static final CompactionManager instance;
    // acquire as read to perform a compaction, and as write to prevent compactions
    private final ReentrantReadWriteLock compactionLock = new ReentrantReadWriteLock();
    // the atomically swapped (not immutable, but should be) set of currently compacting sstables
    private final AtomicReference<HashSet<SSTableReader>> compacting =
        new AtomicReference<HashSet<SSTableReader>>(new HashSet<SSTableReader>());

    static
    {
        instance = new CompactionManager();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private CompactionExecutor executor = new CompactionExecutor();
    private Map<ColumnFamilyStore, Integer> estimatedCompactions = new NonBlockingHashMap<ColumnFamilyStore, Integer>();

    /**
     * @return A lock, for which acquisition means no compactions can run.
     * TODO: should replace this lock and the flusher lock with a CAS'd list of sstables.
     */
    public Lock getCompactionLock()
    {
        return compactionLock.writeLock();
    }

    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) since the compactions are single-threaded,
     * and if a call is unnecessary, it will just be no-oped in the bucketing phase.
     */
    public Future<Integer> submitMinorIfNeeded(final ColumnFamilyStore cfs)
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            public Integer call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    if (cfs.isInvalid())
                        return 0;
                    Integer minThreshold = cfs.getMinimumCompactionThreshold();
                    Integer maxThreshold = cfs.getMaximumCompactionThreshold();
    
                    if (minThreshold == 0 || maxThreshold == 0)
                    {
                        logger.debug("Compaction is currently disabled.");
                        return 0;
                    }
                    logger.debug("Checking to see if compaction of " + cfs.columnFamily + " would be useful");
                    Set<List<SSTableReader>> buckets = getBuckets(convertSSTablesToPairs(cfs.getSSTables()), 50L * 1024L * 1024L);
                    updateEstimateFor(cfs, buckets);
                    int gcBefore = cfs.isIndex()
                                    ? Integer.MAX_VALUE
                                    : (int) (System.currentTimeMillis() / 1000) - cfs.metadata.getGcGraceSeconds();
                    
                    for (List<SSTableReader> sstables : buckets)
                    {
                        if (sstables.size() < minThreshold)
                            continue;
                        // if we have too many to compact all at once, compact older ones first -- this avoids
                        // re-compacting files we just created.
                        Collections.sort(sstables);
                        Collection<SSTableReader> tocompact = cfs.getSSTableTracker().markCompacting(sstables, minThreshold, maxThreshold);
                        if (tocompact == null)
                            // enough threads are busy in this bucket
                            continue;
                        try
                        {
                            return doCompaction(cfs, tocompact, gcBefore);
                        }
                        finally
                        {
                            cfs.getSSTableTracker().unmarkCompacting(tocompact);
                        }
                    }
                }
                finally 
                {
                    compactionLock.readLock().unlock();
                }
                return 0;
            }
        };
        return executor.submit(callable);
    }

    private void updateEstimateFor(ColumnFamilyStore cfs, Set<List<SSTableReader>> buckets)
    {
        Integer minThreshold = cfs.getMinimumCompactionThreshold();
        Integer maxThreshold = cfs.getMaximumCompactionThreshold();

        if (minThreshold > 0 && maxThreshold > 0)
        {
            int n = 0;
            for (List<SSTableReader> sstables : buckets)
            {
                if (sstables.size() >= minThreshold)
                {
                    n += Math.ceil((double)sstables.size() / maxThreshold);
                }
            }
            estimatedCompactions.put(cfs, n);
        }
        else
        {
            logger.debug("Compaction is currently disabled.");
        }
    }

    public void performCleanup(final ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.readLock().lock();
                try 
                {
                    if (!cfStore.isInvalid())
                        doCleanupCompaction(cfStore);
                    return this;
                }
                finally 
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        executor.submit(runnable).get();
    }

    public void performMajor(final ColumnFamilyStore cfStore) throws InterruptedException, ExecutionException
    {
        submitMajor(cfStore, 0, (int) (System.currentTimeMillis() / 1000) - cfStore.metadata.getGcGraceSeconds()).get();
    }

    public Future<Object> submitMajor(final ColumnFamilyStore cfStore, final long skip, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    if (cfStore.isInvalid())
                        return this;
                    Collection<SSTableReader> sstables;
                    if (skip > 0)
                    {
                        sstables = new ArrayList<SSTableReader>();
                        for (SSTableReader sstable : cfStore.getSSTables())
                        {
                            if (sstable.length() < skip * 1024L * 1024L * 1024L)
                            {
                                sstables.add(sstable);
                            }
                        }
                    }
                    else
                    {
                        sstables = cfStore.getSSTables();
                    }
                    
                    Collection<SSTableReader> tocompact = cfStore.getSSTableTracker().markCompacting(sstables, 0, Integer.MAX_VALUE);
                    if (tocompact == null || tocompact.isEmpty())
                        return this;
                    try
                    {
                        doCompaction(cfStore, tocompact, gcBefore);
                    }
                    finally
                    {
                        cfStore.getSSTableTracker().unmarkCompacting(tocompact);
                    }
                    return this;
                }
                finally 
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        return executor.submit(callable);
    }

    public void forceUserDefinedCompaction(String ksname, String dataFiles)
    {
        if (!DatabaseDescriptor.getTables().contains(ksname))
            throw new IllegalArgumentException("Unknown keyspace " + ksname);

        File directory = new File(ksname);
        String[] filenames = dataFiles.split(",");
        Collection<Descriptor> descriptors = new ArrayList<Descriptor>(filenames.length);

        String cfname = null;
        for (String filename : filenames)
        {
            Pair<Descriptor, String> p = Descriptor.fromFilename(directory, filename.trim());
            if (!p.right.equals(Component.DATA.name()))
            {
                throw new IllegalArgumentException(filename + " does not appear to be a data file");
            }
            if (cfname == null)
            {
                cfname = p.left.cfname;
            }
            else if (!cfname.equals(p.left.cfname))
            {
                throw new IllegalArgumentException("All provided sstables should be for the same column family");
            }

            descriptors.add(p.left);
        }

        ColumnFamilyStore cfs = Table.open(ksname).getColumnFamilyStore(cfname);
        submitUserDefined(cfs, descriptors, (int) (System.currentTimeMillis() / 1000) - cfs.metadata.getGcGraceSeconds());
    }

    private Future<Object> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    if (cfs.isInvalid())
                        return this;

                    // look up the sstables now that we're on the compaction executor, so we don't try to re-compact
                    // something that was already being compacted earlier.
                    Collection<SSTableReader> sstables = new ArrayList<SSTableReader>();
                    for (Descriptor desc : dataFiles)
                    {
                        // inefficient but not in a performance sensitive path
                        SSTableReader sstable = lookupSSTable(cfs, desc);
                        if (sstable == null)
                        {
                            logger.info("Will not compact {}: it is not an active sstable", desc);
                        }
                        else
                        {
                            sstables.add(sstable);
                        }
                    }

                    if (sstables.isEmpty())
                    {
                        logger.error("No file to compact for user defined compaction");
                    }
                    // attempt to schedule the set
                    else if ((sstables = cfs.getSSTableTracker().markCompacting(sstables, 1, Integer.MAX_VALUE)) != null)
                    {
                        // success: perform the compaction
                        try
                        {
                            doCompaction(cfs, sstables, gcBefore);
                        }
                        finally
                        {
                            cfs.getSSTableTracker().unmarkCompacting(sstables);
                        }
                    }
                    else
                    {
                        logger.error("SSTables for user defined compaction are already being compacted.");
                    }

                    return this;
                }
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        return executor.submit(callable);
    }

    private SSTableReader lookupSSTable(final ColumnFamilyStore cfs, Descriptor descriptor)
    {
        for (SSTableReader sstable : cfs.getSSTables())
        {
            // .equals() with no other changes won't work because in sstable.descriptor, the directory is an absolute path.
            // We could construct descriptor with an absolute path too but I haven't found any satisfying way to do that
            // (DB.getDataFileLocationForTable() may not return the right path if you have multiple volumes). Hence the
            // endsWith.
            if (sstable.descriptor.toString().endsWith(descriptor.toString()))
                return sstable;
        }
        return null;
    }

    /**
     * Does not mutate data, so is not scheduled.
     */
    public Future<Object> submitValidation(final ColumnFamilyStore cfStore, final AntiEntropyService.Validator validator)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    if (!cfStore.isInvalid())
                        doValidationCompaction(cfStore, validator);
                    return this;
                }
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        return executor.submit(callable);
    }

    /* Used in tests. */
    public void disableAutoCompaction()
    {
        for (String ksname : DatabaseDescriptor.getNonSystemTables())
        {
            Table ks = Table.open(ksname);
            for (ColumnFamilyStore cfs : ks.columnFamilyStores.values())
                cfs.disableAutoCompaction();
        }
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     */
    int doCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore) throws IOException
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert sstables != null;

        Table table = cfs.table;
        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            table.snapshot("compact-" + cfs.columnFamily);

        // sanity check: all sstables must belong to the same cfs
        logger.info("Compacting [" + StringUtils.join(sstables, ",") + "]");
        for (SSTableReader sstable : sstables)
            assert sstable.descriptor.cfname.equals(cfs.columnFamily);

        String compactionFileLocation = table.getDataFileLocation(cfs.getExpectedCompactedFileSize(sstables));
        // If the compaction file path is null that means we have no space left for this compaction.
        // try again w/o the largest one.
        List<SSTableReader> smallerSSTables = new ArrayList<SSTableReader>(sstables);
        while (compactionFileLocation == null && smallerSSTables.size() > 1)
        {
            logger.warn("insufficient space to compact all requested files " + StringUtils.join(smallerSSTables, ", "));
            smallerSSTables.remove(cfs.getMaxSizeFile(smallerSSTables));
            compactionFileLocation = table.getDataFileLocation(cfs.getExpectedCompactedFileSize(smallerSSTables));
        }
        if (compactionFileLocation == null)
        {
            logger.error("insufficient space to compact even the two smallest files, aborting");
            return 0;
        }
        sstables = smallerSSTables;

        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        boolean major = cfs.isCompleteSSTables(sstables);

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        // TODO the int cast here is potentially buggy
        int expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(), (int)SSTableReader.getApproximateKeyCount(sstables));
        if (logger.isDebugEnabled())
          logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        SSTableWriter writer;
        CompactionIterator ci = new CompactionIterator(cfs, sstables, gcBefore, major); // retain a handle so we can call close()
        Iterator<AbstractCompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());
        executor.beginCompaction(cfs.columnFamily, ci);

        Map<DecoratedKey, Long> cachedKeys = new HashMap<DecoratedKey, Long>();

        try
        {
            if (!nni.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                cfs.markCompacted(sstables);
                return 0;
            }

            String newFilename = new File(cfs.getTempSSTablePath(compactionFileLocation)).getAbsolutePath();
            writer = new SSTableWriter(newFilename, expectedBloomFilterSize, cfs.metadata, cfs.partitioner);
            while (nni.hasNext())
            {
                AbstractCompactedRow row = nni.next();
                long position = writer.append(row);
                totalkeysWritten++;

                for (SSTableReader sstable : sstables)
                {
                    if (sstable.getCachedPosition(row.key) != null)
                    {
                        cachedKeys.put(row.key, position);
                        break;
                    }
                }
            }
        }
        finally
        {
            ci.close();
        }

        SSTableReader ssTable = writer.closeAndOpenReader(getMaxDataAge(sstables));
        cfs.replaceCompactedSSTables(sstables, Arrays.asList(ssTable));
        for (Entry<DecoratedKey, Long> entry : cachedKeys.entrySet())
            ssTable.cacheKey(entry.getKey(), entry.getValue());
        submitMinorIfNeeded(cfs);

        long dTime = System.currentTimeMillis() - startTime;
        long startsize = SSTable.getTotalBytes(sstables);
        long endsize = ssTable.length();
        double ratio = (double)endsize / (double)startsize;
        logger.info(String.format("Compacted to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys.  Time: %,dms.",
                                  writer.getFilename(), startsize, endsize, (int) (ratio * 100), totalkeysWritten, dTime));
        return sstables.size();
    }

    private static long getMaxDataAge(Collection<SSTableReader> sstables)
    {
        long max = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
    }

    /**
     * This function goes over each file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupCompaction(ColumnFamilyStore cfs) throws IOException
    {
        assert !cfs.isIndex();
        Table table = cfs.table;
        Collection<Range> ranges = StorageService.instance.getLocalRanges(table.name);

        for (SSTableReader sstable : cfs.getSSTables())
        {
            if (cfs.getSSTableTracker().markCompacting(Arrays.asList(sstable), 1, 1) == null)
            {
                // TODO: we should submit each sstable as an independent compaction,
                // rather than compacting them all sequentially inside one task.
                logger.warn("Could not perform cleanup compaction for " + sstable + ": it is already being compacted.");
                continue;
            }
            long totalkeysWritten = 0;
            long startTime = System.currentTimeMillis();
            SSTableWriter writer = null;
            try
            {
                logger.info("Cleaning up " + sstable);
                // Calculate the expected compacted filesize
                long expectedRangeFileSize = cfs.getExpectedCompactedFileSize(Arrays.asList(sstable)) / 2;
                String compactionFileLocation = table.getDataFileLocation(expectedRangeFileSize);
                if (compactionFileLocation == null)
                    throw new IOException("disk full");

                int expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(),
                                                    (int)(SSTableReader.getApproximateKeyCount(Arrays.asList(sstable)) / 2));
                if (logger.isDebugEnabled())
                logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

                SSTableScanner scanner = sstable.getDirectScanner(CompactionIterator.FILE_BUFFER_SIZE);
                SortedSet<ByteBuffer> indexedColumns = cfs.getIndexedColumns();
                executor.beginCompaction(cfs.columnFamily, new CleanupInfo(sstable, scanner));
                try
                {
                    while (scanner.hasNext())
                    {
                        SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                        if (Range.isTokenInRanges(row.getKey().token, ranges))
                        {
                            writer = maybeCreateWriter(cfs, compactionFileLocation, expectedBloomFilterSize, writer);
                            writer.append(new EchoedRow(row));
                            totalkeysWritten++;
                        }
                        else
                        {
                            while (row.hasNext())
                            {
                                IColumn column = row.next();
                                if (indexedColumns.contains(column.name()))
                                    Table.cleanupIndexEntry(cfs, row.getKey().key, column);
                            }
                        }
                    }
                }
                finally
                {
                    scanner.close();
                }
            }
            finally
            {
                cfs.getSSTableTracker().unmarkCompacting(Arrays.asList(sstable));
            }

            List<SSTableReader> results = new ArrayList<SSTableReader>();
            if (writer != null)
            {
                SSTableReader newSstable = writer.closeAndOpenReader(sstable.maxDataAge);
                results.add(newSstable);

                String format = "Cleaned up to %s.  %,d to %,d (~%d%% of original) bytes for %,d keys.  Time: %,dms.";
                long dTime = System.currentTimeMillis() - startTime;
                long startsize = sstable.length();
                long endsize = newSstable.length();
                double ratio = (double)endsize / (double)startsize;
                logger.info(String.format(format, writer.getFilename(), startsize, endsize, (int)(ratio*100), totalkeysWritten, dTime));
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            for (ByteBuffer columnName : cfs.getIndexedColumns())
            {
                try
                {
                    cfs.getIndexedColumnFamilyStore(columnName).forceBlockingFlush();
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
            cfs.replaceCompactedSSTables(Arrays.asList(sstable), results);
        }
    }

    private SSTableWriter maybeCreateWriter(ColumnFamilyStore cfs, String compactionFileLocation, int expectedBloomFilterSize, SSTableWriter writer)
            throws IOException
    {
        if (writer == null)
        {
            FileUtils.createDirectory(compactionFileLocation);
            String newFilename = new File(cfs.getTempSSTablePath(compactionFileLocation)).getAbsolutePath();
            writer = new SSTableWriter(newFilename, expectedBloomFilterSize, cfs.metadata, cfs.partitioner);
        }
        return writer;
    }

    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
    private void doValidationCompaction(ColumnFamilyStore cfs, AntiEntropyService.Validator validator) throws IOException
    {
        // flush first so everyone is validating data that is as similar as possible
        try
        {
            StorageService.instance.forceTableFlush(cfs.table.name, cfs.getColumnFamilyName());
        }
        catch (ExecutionException e)
        {
            throw new IOException(e);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        CompactionIterator ci = new ValidationCompactionIterator(cfs);
        executor.beginCompaction(cfs.columnFamily, ci);
        try
        {
            Iterator<AbstractCompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());

            // validate the CF as we iterate over it
            validator.prepare(cfs);
            while (nni.hasNext())
            {
                AbstractCompactedRow row = nni.next();
                validator.add(row);
            }
            validator.complete();
        }
        finally
        {
            ci.close();
        }
    }

    /*
    * Group files of similar size into buckets.
    */
    static <T> Set<List<T>> getBuckets(Collection<Pair<T, Long>> files, long min)
    {
        // Sort the list in order to get deterministic results during the grouping below
        List<Pair<T, Long>> sortedFiles = new ArrayList<Pair<T, Long>>(files);
        Collections.sort(sortedFiles, new Comparator<Pair<T, Long>>()
        {
            public int compare(Pair<T, Long> p1, Pair<T, Long> p2)
            {
                return p1.right.compareTo(p2.right);
            }
        });

        Map<List<T>, Long> buckets = new HashMap<List<T>, Long>();

        for (Pair<T, Long> pair: sortedFiles)
        {
            long size = pair.right;

            boolean bFound = false;
            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `min`)
            for (Entry<List<T>, Long> entry : buckets.entrySet())
            {
                List<T> bucket = entry.getKey();
                long averageSize = entry.getValue();
                if ((size > (averageSize / 2) && size < (3 * averageSize) / 2)
                    || (size < min && averageSize < min))
                {
                    // remove and re-add because adding changes the hash
                    buckets.remove(bucket);
                    long totalSize = bucket.size() * averageSize;
                    averageSize = (totalSize + size) / (bucket.size() + 1);
                    bucket.add(pair.left);
                    buckets.put(bucket, averageSize);
                    bFound = true;
                    break;
                }
            }
            // no similar bucket found; put it in a new one
            if (!bFound)
            {
                ArrayList<T> bucket = new ArrayList<T>();
                bucket.add(pair.left);
                buckets.put(bucket, size);
            }
        }

        return buckets.keySet();
    }

    private static Collection<Pair<SSTableReader, Long>> convertSSTablesToPairs(Collection<SSTableReader> collection)
    {
        Collection<Pair<SSTableReader, Long>> tablePairs = new ArrayList<Pair<SSTableReader, Long>>();
        for(SSTableReader table: collection)
        {
            tablePairs.add(new Pair<SSTableReader, Long>(table, table.length()));
        }
        return tablePairs;
    }
    
    /**
     * Is not scheduled, because it is performing disjoint work from sstable compaction.
     */
    public Future submitIndexBuild(final ColumnFamilyStore cfs, final Table.IndexBuilder builder)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                compactionLock.readLock().lock();
                try
                {
                    if (cfs.isInvalid())
                        return;
                    executor.beginCompaction(cfs.columnFamily, builder);
                    builder.build();
                }
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        
        // don't submit to the executor if the compaction lock is held by the current thread. Instead return a simple
        // future that will be immediately immediately get()ed and executed. Happens during a migration, which locks
        // the compaction thread and then reinitializes a ColumnFamilyStore. Under normal circumstances, CFS spawns
        // index jobs to the compaction manager (this) and blocks on them.
        if (compactionLock.isWriteLockedByCurrentThread())
            return new SimpleFuture(runnable);
        else
            return executor.submit(runnable);
    }

    /**
     * Submits an sstable to be rebuilt: is not scheduled, since the sstable must not exist.
     */
    public Future<SSTableReader> submitSSTableBuild(Descriptor desc, OperationType type)
    {
        // invalid descriptions due to missing or dropped CFS are handled by SSTW and StreamInSession.
        final SSTableWriter.Builder builder = SSTableWriter.createBuilder(desc, type);
        Callable<SSTableReader> callable = new Callable<SSTableReader>()
        {
            public SSTableReader call() throws IOException
            {
                compactionLock.readLock().lock();
                try
                {
                    executor.beginCompaction(builder.cfs.columnFamily, builder);
                    return builder.build();
                }
                finally
                {
                    compactionLock.readLock().unlock();
                }
            }
        };
        return executor.submit(callable);
    }

    public Future<?> submitCacheWrite(final CacheWriter writer)
    {
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                executor.beginCompaction(writer.getColumnFamily(), writer);
                writer.saveCache();
            }
        };
        return executor.submit(runnable);
    }

    private static class ValidationCompactionIterator extends CompactionIterator
    {
        public ValidationCompactionIterator(ColumnFamilyStore cfs) throws IOException
        {
            super(cfs, cfs.getSSTables(), (int) (System.currentTimeMillis() / 1000) - cfs.metadata.getGcGraceSeconds(), true);
        }

        @Override
        public String getTaskType()
        {
            return "Validation";
        }
    }

    public void checkAllColumnFamilies() throws IOException
    {
        // perform estimates
        for (final ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            Runnable runnable = new Runnable()
            {
                public void run ()
                {
                    logger.debug("Estimating compactions for " + cfs.columnFamily);
                    final Set<List<SSTableReader>> buckets = getBuckets(convertSSTablesToPairs(cfs.getSSTables()), 50L * 1024L * 1024L);
                    updateEstimateFor(cfs, buckets);
                }
            };
            executor.submit(runnable);
        }

        // actually schedule compactions.  done in a second pass so all the estimates occur before we
        // bog down the executor in actual compactions.
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            submitMinorIfNeeded(cfs);
        }
    }

    private static class CompactionExecutor extends DebuggableThreadPoolExecutor
    {
        private volatile String columnFamily;
        private volatile ICompactionInfo ci;

        public CompactionExecutor()
        {
            // FIXME: this should be a legit stage eventually: we're duping stage management
            super(Math.max(2, Runtime.getRuntime().availableProcessors()),
                  Math.max(2, Runtime.getRuntime().availableProcessors()),
                  60,
                  TimeUnit.SECONDS,
                  new LinkedBlockingQueue<Runnable>(),
                  new NamedThreadFactory("CompactionExecutor", DatabaseDescriptor.getCompactionThreadPriority()));
        }

        @Override
        public void afterExecute(Runnable r, Throwable t)
        {
            super.afterExecute(r, t);
            columnFamily = null;
            ci = null;
        }

        void beginCompaction(String columnFamily, ICompactionInfo ci)
        {
            this.columnFamily = columnFamily;
            this.ci = ci;
        }

        public String getColumnFamilyName()
        {
            return columnFamily == null ? null : columnFamily;
        }

        public Long getBytesTotal()
        {
            return ci == null ? null : ci.getTotalBytes();
        }

        public Long getBytesCompleted()
        {
            return ci == null ? null : ci.getBytesComplete();
        }

        public String getType()
        {
            return ci == null ? null : ci.getTaskType();
        }
    }

    public String getColumnFamilyInProgress()
    {
        return executor.getColumnFamilyName();
    }

    public Long getBytesTotalInProgress()
    {
        return executor.getBytesTotal();
    }

    public Long getBytesCompacted()
    {
        return executor.getBytesCompleted();
    }

    public String getCompactionType()
    {
        return executor.getType();
    }

    public int getPendingTasks()
    {
        int n = 0;
        for (Integer i : estimatedCompactions.values())
            n += i;
        return (int) (executor.getTaskCount() - executor.getCompletedTaskCount()) + n;
    }

    public long getCompletedTasks()
    {
        return executor.getCompletedTaskCount();
    }
    
    private static class SimpleFuture implements Future
    {
        private Runnable runnable;
        
        private SimpleFuture(Runnable r) 
        {
            runnable = r;
        }
        
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            throw new IllegalStateException("May not call SimpleFuture.cancel()");
        }

        public boolean isCancelled()
        {
            return false;
        }

        public boolean isDone()
        {
            return runnable == null;
        }

        public Object get() throws InterruptedException, ExecutionException
        {
            runnable.run();
            runnable = null;
            return runnable;
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            throw new IllegalStateException("May not call SimpleFuture.get(long, TimeUnit)");
        }
    }

    private static class EchoedRow extends AbstractCompactedRow
    {
        private final SSTableIdentityIterator row;

        public EchoedRow(SSTableIdentityIterator row)
        {
            super(row.getKey());
            this.row = row;
        }

        public void write(DataOutput out) throws IOException
        {
            out.writeLong(row.dataSize);
            row.echoData(out);
        }

        public void update(MessageDigest digest)
        {
            // EchoedRow is not used in anti-entropy validation
            throw new UnsupportedOperationException();
        }

        public boolean isEmpty()
        {
            return !row.hasNext();
        }

        public int columnCount()
        {
            return row.columnCount;
        }
    }

    private static class CleanupInfo implements ICompactionInfo
    {
        private final SSTableReader sstable;
        private final SSTableScanner scanner;

        public CleanupInfo(SSTableReader sstable, SSTableScanner scanner)
        {
            this.sstable = sstable;
            this.scanner = scanner;
        }

        public long getTotalBytes()
        {
            return scanner.getFileLength();
        }

        public long getBytesComplete()
        {
            return scanner.getFilePointer();
        }

        public String getTaskType()
        {
            return "Cleanup of " + sstable.getColumnFamilyName();
        }
    }
}
