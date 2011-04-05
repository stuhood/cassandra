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
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
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
    private final Decorator decorator;

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
        this.decorator = new Decorator();
    }

    static long estimateRowsFromIndex(RandomAccessFile ifile) throws IOException
    {
        // FIXME
        return 1000;
    }

    /**
     * Reads index blocks to return keys sequentially.
     * TODO: Using the system default partitioner: will not iterate secindexes
     */
    public static KeyIterator getKeyIterator(Descriptor desc)
    {
        CFMetaData cfm = DatabaseDescriptor.getCFMetaData(desc.ksname, desc.cfname);
        final List<AbstractType> types = cfm.getTypes();
        return new KeyIterator(desc)
        {
            final ArrayDeque<ByteBuffer> keys = new ArrayDeque<ByteBuffer>();
            protected DecoratedKey computeNext()
            {
                if (keys.isEmpty())
                {
                    try
                    {
                        if (in.isEOF())
                            return endOfData();
                        readBlock(types, in, keys, null);
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }
                return StorageService.getPartitioner().decorateKey(keys.removeFirst());
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
            List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
            List<RowHeader> headers = new ArrayList<RowHeader>();
            // read from the index a block at a time
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                readBlock(types, input, keys, headers);
                // add the first position in the block to the summary
                // (TODO: would need to shimmy a bit to respect the index interval at read time:
                // getPositionFromIndex assumes only one block needs to be scanned)
                indexSummary.addEntry(partitioner.decorateKey(keys.get(0)), indexPosition);
                ibuilder.addPotentialBoundary(indexPosition);

                for (int i = 0; i < keys.size(); i++)
                {
                    dbuilder.addPotentialBoundary(headers.get(i).position());
                    if (recreatebloom)
                        bf.add(keys.get(i));
                    if (!cacheLoading)
                        continue;
                    DecoratedKey key = partitioner.decorateKey(keys.get(i));
                    if (keysToLoadInCache.contains(key))
                        cacheKey(key, headers.get(i));
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
    protected RowHeader getPositionFromIndex(Position sampledPosition, DecoratedKey decoratedKey, Operator op)
    {
        // read the sampled block from the on-disk index
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition.position, INDEX_FILE_BUFFER_BYTES);
        List<ByteBuffer> keybuff = new ArrayList<ByteBuffer>();
        List<RowHeader> headers = new ArrayList<RowHeader>();
        FileDataInput input = segments.next();
        try
        {
            // read a block: the key exists in this block, or not at all
            readBlock(types, input, keybuff, headers);
            // lazily decorate keys and search for the first gte
            List<DecoratedKey> keys = Lists.transform(keybuff, decorator);
            int b = Collections.binarySearch(keys, decoratedKey, DecoratedKey.comparator);
            int comparison;
            if (b >= 0)
                // exact match
                comparison = 0;
            else
            {
                // round up to first entry gt the match
                b = -(b + 1);
                if (b >= keys.size())
                    // there is nothing gte the key
                    return miss(op);
                comparison = 1;
            }

            // compare to match
            int v = op.apply(comparison);
            if (v == 0)
            {
                // hit!
                if (comparison == 0 && keyCache != null && keyCache.getCapacity() > 0)
                {
                    if (op == Operator.EQ)
                        bloomFilterTracker.addTruePositive();
                    // store exact match for the key
                    cacheKey(keys.get(b), headers.get(b));
                }
                return headers.get(b);
            }
            else if (v > 0 && b + 1 < keys.size())
            {
                // entry would like to match forward: and there is an entry after the match
                return headers.get(b + 1);
            }
            // else miss
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            FileUtils.closeQuietly(input);
        }
        return miss(op);
    }

    private RowHeader miss(Operator op)
    {
        if (op == Operator.EQ)
            bloomFilterTracker.addFalsePositive();
        return null;
    }

    /**
     * Read a block of keys and row headers from the index.
     * This method is the inverse of NestedIndexWriter.flush
     * @param keysout An output collection of row keys.
     * @param headout An output collection of headers corresponding to the keys, or null.
     */
    private static void readBlock(List<AbstractType> types, DataInput from, Collection<ByteBuffer> keysout, Collection<RowHeader> headout) throws IOException
    {
        // keys and metadata
        keysout.clear();
        types.get(0).decompress(VERSION, from, keysout);
        long[] markedForDeleteAt = LongType.decode(from);
        long[] localDeletionTime = LongType.decode(from);
        assert keysout.size() == markedForDeleteAt.length : keysout.size() + " vs " + markedForDeleteAt.length;

        // first name level
        ArrayList<ByteBuffer> names = new ArrayList<ByteBuffer>();
        Observer.IndexedBitSet parents = Observer.IndexedBitSet.from(LongType.decode(from));
        types.get(1).decompress(VERSION, from, names);
        // skip the remaining levels (TODO: since we don't currently allow random
        // access to blocks, they are not useful yet)
        for (int i = 2; i < types.size(); i++)
        {
            // TODO: need primitive skip on type specific compression
            LongType.decode(from);
            types.get(i).decompress(VERSION, from, new ArrayList<ByteBuffer>());
        }
        assert names.size() == (parents.index - keysout.size()) :
            names.size() + " vs (" + parents.index + " - " + keysout.size() + ")";

        // decode the metadata and offsets using the LongType primitive
        long[] offsets = LongType.decode(from);
        
        // finished reading from disk: should we reconstruct the headers?
        if (headout == null)
            return;
        headout.clear();

        /**
         * reconstruct positions using the keys, parent flags and offsets:
         * runs of equal parent bits are entries for a particular parent.
         * example: 1 consecutive parent bit represents a narrow row, where
         * we didn't bother to record children. 2 consecutive bits represents
         * a parent with exactly one child (which is both the min and max) and
         * 3 bits or more represent 2 or more children. A simple RowHeader will
         * be created for 1 or 2 bits: a NestedRowHeader will be created for 3
         * or more.
         */
        int keyidx = 0, nameidx = 0;
        int start = 0;
        assert parents.index < Integer.MAX_VALUE;
        while (start < parents.index)
        {
            int end = (int)parents.endOfRun(start);
            int runLength = end - start;
            switch (runLength)
            {
                case 1:
                    // tombstone or narrow row: one offset, zero names
                    headout.add(new RowHeader(offsets[start]));
                    break;
                case 2:
                    // row of exactly one column: two offsets, one name
                    // (unusual: we would normally just encode as narrow)
                    headout.add(new RowHeader(offsets[start]));
                    nameidx++;
                    break;
                default:
                    // wide row
                    // the first column for the run is the minimum: the last is the max
                    ByteBuffer min = names.get(nameidx);
                    headout.add(new NestedRowHeader(offsets[start],
                                                    markedForDeleteAt[keyidx],
                                                    (int)localDeletionTime[keyidx],
                                                    min,
                                                    names.get(nameidx + runLength - 2)));
                    nameidx += runLength - 1;
            }
            // reset for next run
            start = end;
            keyidx++;
        }
    }

    private final class Decorator implements Function<ByteBuffer,DecoratedKey>
    {
        public DecoratedKey apply(ByteBuffer in)
        {
            return partitioner.decorateKey(in);
        }
    }
}
