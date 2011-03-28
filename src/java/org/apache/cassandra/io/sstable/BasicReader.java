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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

/**
 * Implements index reading for the basic index containing keys and offsets.
 */
public class BasicReader extends SSTableReader
{
    // guesstimated size of INDEX_INTERVAL index entries
    private static final int INDEX_FILE_BUFFER_BYTES = 16 * DatabaseDescriptor.getIndexInterval();

    BasicReader(Descriptor desc,
                     Set<Component> components,
                     CFMetaData metadata,
                     IPartitioner partitioner,
                     SegmentedFile ifile,
                     SegmentedFile dfile,
                     IndexSummary indexSummary,
                     Filter bloomFilter,
                     long maxDataAge,
                     SSTableMetadata sstableMetadata)
    throws IOException
    {
        super(desc, components, metadata, partitioner, ifile, dfile, indexSummary, bloomFilter, maxDataAge, sstableMetadata);
    }

    static long estimateRowsFromIndex(RandomAccessReader ifile) throws IOException
    {
        // collect sizes for the first 10000 keys, or first 10 megabytes of data
        final int SAMPLES_CAP = 10000, BYTES_CAP = (int)Math.min(10000000, ifile.length());
        int keys = 0;
        while (ifile.getFilePointer() < BYTES_CAP && keys < SAMPLES_CAP)
        {
            ByteBufferUtil.skipShortLength(ifile);
            FileUtils.skipBytesFully(ifile, 8);
            keys++;
        }
        assert keys > 0 && ifile.getFilePointer() > 0 && ifile.length() > 0 : "Unexpected empty index file: " + ifile;
        long estimatedRows = ifile.length() / (ifile.getFilePointer() / keys);
        ifile.seek(0);
        return estimatedRows;
    }

    public static KeyIterator getKeyIterator(Descriptor desc)
    {
        return new KeyIterator(desc)
        {
            protected DecoratedKey computeNext()
            {
                try
                {
                    if (in.isEOF())
                        return endOfData();
                    DecoratedKey key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, ByteBufferUtil.readWithShortLength(in));
                    in.readLong(); // skip data position
                    return key;
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public List<Pair<Long,Long>> getPositionsForRanges(Collection<Range> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Long,Long>> positions = new ArrayList<Pair<Long,Long>>();
        for (AbstractBounds range : AbstractBounds.normalize(ranges))
        {
            BlockHeader left = getPosition(new DecoratedKey(range.left, null), Operator.GT);
            if (left == null)
                // left is past the end of the file
                continue;
            BlockHeader right = getPosition(new DecoratedKey(range.right, null), Operator.GT);
            long rightPos = right != null && !Range.isWrapAround(range.left, range.right) ?
                right.position() :
                // right is past the end of the file, or it wraps
                length();
            if (left.position() == rightPos)
                // empty range
                continue;
            positions.add(new Pair(Long.valueOf(left.position()), Long.valueOf(rightPos)));
        }
        return positions;
    }

    @Override
    protected void load(Set<DecoratedKey> keysToLoadInCache) throws IOException
    {
        boolean recreatebloom = descriptor.version.hasStringsInBloomFilter;
        if (!recreatebloom)
            loadBloomFilter();
        boolean cacheLoading = keyCache != null && !keysToLoadInCache.isEmpty();
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = (components.contains(Component.COMPRESSION_INFO))
                                          ? SegmentedFile.getCompressedBuilder()
                                          : SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());

        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        RandomAccessReader input = RandomAccessReader.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)), true);
        try
        {
            if (keyCache != null && keyCache.getCapacity() - keyCache.size() < keysToLoadInCache.size())
                keyCache.updateCapacity(keyCache.size() + keysToLoadInCache.size());

            long indexSize = input.length();
            long estimatedKeys = estimateRowsFromIndex(input);
            indexSummary = new IndexSummary(estimatedKeys);
            if (recreatebloom)
                // estimate key count based on index length
                bf = LegacyBloomFilter.getFilter(estimatedKeys, 15);
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                boolean shouldAddEntry = indexSummary.shouldAddEntry();
                ByteBuffer key = (shouldAddEntry || cacheLoading || recreatebloom)
                             ? ByteBufferUtil.readWithShortLength(input)
                             : ByteBufferUtil.skipShortLength(input);
                long dataPosition = input.readLong();
                if (key != null)
                {
                    DecoratedKey decoratedKey = decodeKey(partitioner, descriptor, key);
                    if (recreatebloom)
                        bf.add(decoratedKey.key);
                    if (shouldAddEntry)
                        indexSummary.addEntry(decoratedKey, indexPosition);
                    if (cacheLoading && keysToLoadInCache.contains(decoratedKey))
                        cacheKey(decoratedKey, new BlockHeader(dataPosition));
                }

                indexSummary.incrementRowid();
                ibuilder.addPotentialBoundary(indexPosition);
                dbuilder.addPotentialBoundary(dataPosition);
            }
            indexSummary.complete();
        }
        finally
        {
            FileUtils.closeQuietly(input);
        }

        // finalize the state of the reader
        ifile = ibuilder.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));
        dfile = dbuilder.complete(descriptor.filenameFor(Component.DATA));
    }

    @Override
    protected List<BlockHeader> getPositionsFromIndex(long sampledPosition, DecoratedKey decoratedKey, Operator op)
    {
        // scan the on-disk index, starting at the nearest sampled position
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition, INDEX_FILE_BUFFER_BYTES);
        while (segments.hasNext())
        {
            FileDataInput input = segments.next();
            try
            {
                while (!input.isEOF())
                {
                    // read key & data position from index entry
                    DecoratedKey indexDecoratedKey = decodeKey(partitioner, descriptor, ByteBufferUtil.readWithShortLength(input));
                    long dataPosition = input.readLong();

                    int comparison = indexDecoratedKey.compareTo(decoratedKey);
                    int v = op.apply(comparison);
                    if (v == 0)
                    {
                        BlockHeader header = new BlockHeader(dataPosition);
                        if (comparison == 0 && keyCache != null && keyCache.getCapacity() > 0 && decoratedKey.key != null)
                        {
                            // store exact match for the key
                            cacheKey(decoratedKey, header);
                        }
                        if (op == Operator.EQ)
                            bloomFilterTracker.addTruePositive();
                        return Collections.singletonList(header);
                    }
                    if (v < 0)
                    {
                        if (op == Operator.EQ)
                            bloomFilterTracker.addFalsePositive();
                        return null;
                    }
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                FileUtils.closeQuietly(input);
            }
        }

        if (op == Operator.EQ)
            bloomFilterTracker.addFalsePositive();
        return null;
    }
}
