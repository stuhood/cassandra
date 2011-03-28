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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

public class NestedReader extends SSTableReader
{
    // guesstimated size of an index block based on count or size thresholds
    private static final int INDEX_FILE_BUFFER_BYTES = Math.min(64 * DatabaseDescriptor.getIndexInterval(),
                                                                DatabaseDescriptor.getColumnIndexSize());

    private static final int VERSION = MessagingService.version_;

    // for each level, a type to be used for compression
    private final List<AbstractType> types;

    NestedReader(Descriptor desc,
                 Set<Component> components,
                 CFMetaData metadata,
                 IPartitioner partitioner,
                 SegmentedFile ifile,
                 SegmentedFile dfile,
                 IndexSummary indexSummary,
                 Filter bloomFilter,
                 long maxDataAge,
                 EstimatedHistogram rowSizes,
                 EstimatedHistogram columnCounts)
    throws IOException
    {
        super(desc, components, metadata, partitioner, ifile, dfile, indexSummary, bloomFilter, maxDataAge, rowSizes, columnCounts);
        this.types = metadata.getTypes();
    }

    static long estimateRowsFromIndex(RandomAccessFile ifile) throws IOException
    {
        // FIXME
        return 1000;
    }

    /**
     * Reads index blocks and performs deduping of keys (since they might be stored in multiple blocks).
     * TODO: Using the system default partitioner: will not iterate secindexes
     */
    public static KeyIterator getKeyIterator(Descriptor desc)
    {
        CFMetaData cfm = DatabaseDescriptor.getCFMetaData(desc.ksname, desc.cfname);
        final List<AbstractType> types = cfm.getTypes();
        return new KeyIterator(desc)
        {
            final ArrayDeque<Position> positions = new ArrayDeque<Position>();
            protected DecoratedKey computeNext()
            {
                if (positions.isEmpty())
                {
                    try
                    {
                        if (in.isEOF())
                            return endOfData();
                        readBlockKeys(StorageService.getPartitioner(), types, in, positions);
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }
                return positions.removeFirst().key;
            }
        };
    }

    @Override
    protected void load(boolean recreatebloom, Set<DecoratedKey> keysToLoadInCache) throws IOException
    {
        boolean cacheLoading = keyCache != null && !keysToLoadInCache.isEmpty();
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());

        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)),
                                                                      "r",
                                                                      BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE,
                                                                      true);
        try
        {
            if (keyCache != null && keyCache.getCapacity() - keyCache.size() < keysToLoadInCache.size())
                keyCache.updateCapacity(keyCache.size() + keysToLoadInCache.size());

            long indexSize = input.length();
            long estimatedKeys = NestedReader.estimateRowsFromIndex(input);
            indexSummary = new IndexSummary(estimatedKeys);
            if (recreatebloom)
                // estimate key count based on index length
                bf = LegacyBloomFilter.getFilter(estimatedKeys, 15);
            List<Position> positions = new ArrayList<Position>();
            // read from the index a block at a time
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                readBlockKeys(partitioner, types, input, positions);

                // add the first position in the block to the summary
                // (TODO: these blocks respect the index interval at write time)
                indexSummary.addEntry(positions.get(0).key, indexPosition);
                ibuilder.addPotentialBoundary(indexPosition);

                for (Position pos : positions)
                {
                    if (recreatebloom)
                        bf.add(pos.key.key);
                    if (cacheLoading && keysToLoadInCache.contains(pos.key))
                        cacheKey(pos.key, pos.position);
                    dbuilder.addPotentialBoundary(pos.position);
                }
            }
        }
        finally
        {
            FileUtils.closeQuietly(input);
        }

        // finalize the state of the reader
        indexSummary.complete();
        ifile = ibuilder.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));
        dfile = dbuilder.complete(descriptor.filenameFor(Component.DATA));
    }

    @Override
    protected long getPositionFromIndex(Position sampledPosition, DecoratedKey decoratedKey, Operator op)
    {
        // read the sampled block from the on-disk index
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition.position, INDEX_FILE_BUFFER_BYTES);
        List<Position> positions = new ArrayList<Position>();
        FileDataInput input = segments.next();
        try
        {
            // read one block: the summary guarantees that the key exists in this
            // block, or not at all
            readBlockKeys(partitioner, types, input, positions);

            int b = Position.binarySearch(positions, decoratedKey);
            if (b < 0)
                // nothing lte in this block
                throw new IOException("IndexSummary sent us to the wrong block: " + sampledPosition + " for " + decoratedKey);

            // compare to the matched entry
            Position matched = positions.get(b);
            int comparison = matched.key.compareTo(decoratedKey);
            int v = op.apply(comparison);
            if (v == 0)
            {
                if (comparison == 0 && keyCache != null && keyCache.getCapacity() > 0)
                {
                    if (op == Operator.EQ)
                        bloomFilterTracker.addTruePositive();
                    // store exact match for the key
                    cacheKey(matched.key, matched.position);
                }
                return matched.position;
            }
            if (v < 0)
            {
                if (op == Operator.EQ)
                    bloomFilterTracker.addFalsePositive();
                return -1;
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

        if (op == Operator.EQ)
            bloomFilterTracker.addFalsePositive();
        return -1;
    }

    /**
     * Read a block of keys from the index, ignoring the rest of the content.
     * This method is the inverse of NestedIndexWriter.flush
     */
    private static void readBlockKeys(IPartitioner partitioner, List<AbstractType> types, DataInput from, Collection<Position> to) throws IOException
    {
        // decode keys
        ArrayList<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        types.get(0).decompress(VERSION, from, keys);

        // record the parent flags of the first name level
        Observer.IndexedBitSet parent = Observer.IndexedBitSet.from(LongType.decode(from));
        // skip the names and remaining levels
        // TODO: needs primitive skip on type specific compression
        types.get(1).decompress(VERSION, from, new ArrayList<ByteBuffer>());
        for (int i = 2; i < types.size(); i++)
        {
            LongType.decode(from);
            types.get(i).decompress(VERSION, from, new ArrayList<ByteBuffer>());
        }

        // decode the offsets using the LongType primitive
        long[] offsets = LongType.decode(from);
        
        // reconstruct positions using the keys, parent flags and offsets
        to.clear();
        Iterator<ByteBuffer> iter = keys.iterator();
        // decode the first key and parent flag
        to.add(new Position(partitioner.decorateKey(iter.next()), offsets[0]));
        for (int i = 1; i < parent.index; i++)
        {
            if (parent.get(i - 1) == parent.get(i))
                continue;
            // the parent of the column changed, record a new position
            to.add(new Position(partitioner.decorateKey(iter.next()), offsets[i]));
        }
    }
}
