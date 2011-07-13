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
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

public class NestedReader extends SSTableReader
{
    // guesstimated size of an index block based on count or size thresholds
    private static final int INDEX_FILE_BUFFER_BYTES = Math.min(64 * DatabaseDescriptor.getIndexInterval(),
                                                                DatabaseDescriptor.getColumnIndexSize());

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
                 SSTableMetadata sstableMetadata)
    throws IOException
    {
        super(desc, components, metadata, partitioner, ifile, dfile, indexSummary, bloomFilter, maxDataAge, sstableMetadata);
        this.types = metadata.getTypes();
        this.decorator = new Decorator();
    }

    /**
     * Reads index blocks to return keys sequentially.
     * TODO: Using the system default partitioner: will not iterate secindexes
     */
    public static KeyIterator getKeyIterator(final Descriptor desc)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(desc.ksname, desc.cfname);
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
                        readIndexBlock(desc, types, in, keys, null);
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

    public List<Pair<Long,Long>> getPositionsForRanges(Collection<Range> ranges)
    {
        throw new RuntimeException("FIXME: not implemented");
    }

    @Override
    protected void load(Set<DecoratedKey> keysToLoadInCache) throws IOException
    {
        loadBloomFilter();
        boolean cacheLoading = keyCache != null && !keysToLoadInCache.isEmpty();
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        RandomAccessReader input = RandomAccessReader.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)), true);
        try
        {
            if (keyCache != null && keyCache.getCapacity() - keyCache.size() < keysToLoadInCache.size())
                keyCache.updateCapacity(keyCache.size() + keysToLoadInCache.size());

            long indexSize = input.length();
            List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
            List<List<BlockHeader>> headers = new ArrayList<List<BlockHeader>>();
            // read from the index a block at a time
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                readIndexBlock(input, keys, headers);

                // first block?
                if (indexSummary == null)
                {
                    // estimate the number of keys in the file
                    long estimatedKeys = indexSize / (input.getFilePointer() / keys.size());
                    indexSummary = new IndexSummary(estimatedKeys);
                }

                // add the first position in the block to the summary
                // (we would need to shimmy a bit to respect the index interval at read time:
                // getPositionFromIndex assumes only one block needs to be scanned)
                indexSummary.addEntry(partitioner.decorateKey(keys.get(0)), indexPosition);
                ibuilder.addPotentialBoundary(indexPosition);
                
                // load the headers in the block
                for (int i = 0; i < keys.size(); i++)
                {
                    List<BlockHeader> rowHeaders = headers.get(i);
                    // boundary at the first block of the row
                    dbuilder.addPotentialBoundary(rowHeaders.get(0).position());
                    if (!cacheLoading)
                        continue;
                    // FIXME: only caching narrow rows
                    DecoratedKey key = partitioner.decorateKey(keys.get(i));
                    if (keysToLoadInCache.contains(key) && rowHeaders.size() == 1)
                        cacheKey(key, rowHeaders.get(0));
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
    protected List<BlockHeader> getPositionsFromIndex(long sampledPosition, DecoratedKey decoratedKey, Operator op)
    {
        // read the sampled block from the on-disk index
        List<ByteBuffer> keybuff = new ArrayList<ByteBuffer>();
        List<List<BlockHeader>> headers = new ArrayList<List<BlockHeader>>();
        FileDataInput input = ifile.iterator(sampledPosition, INDEX_FILE_BUFFER_BYTES).next();
        try
        {
            // read a block: the key exists in this block, or not at all
            readIndexBlock(input, keybuff, headers);
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
                if (op == Operator.EQ)
                    bloomFilterTracker.addTruePositive();
                List<BlockHeader> rowHeaders = headers.get(b);
                // FIXME: only caching narrow rows
                if (comparison == 0 && keyCache != null && keyCache.getCapacity() > 0 && rowHeaders.size() == 1)
                    // store exact match for the key
                    cacheKey(keys.get(b), rowHeaders.get(0));
                return rowHeaders;
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

    private List<BlockHeader> miss(Operator op)
    {
        if (op == Operator.EQ)
            bloomFilterTracker.addFalsePositive();
        return null;
    }

    private void readIndexBlock(DataInput from, Collection<ByteBuffer> keysout, Collection<List<BlockHeader>> headout) throws IOException
    {
        readIndexBlock(descriptor, types, from, keysout, headout);
    }

    /**
     * Read a block of keys and their headers from the index.
     * This method is the inverse of NestedIndexWriter.flush
     * @param keysout An output collection of row keys.
     * @param headout An output collection of lists of headers per corresponding key, or null
     */
    private static void readIndexBlock(Descriptor desc,
                                       List<AbstractType> types,
                                       DataInput from,
                                       Collection<ByteBuffer> keysout,
                                       Collection<List<BlockHeader>> headout) throws IOException
    {
        // read the index block
        ByteBuffer inbuff = ByteBufferUtil.readWithLength(from);

        // decode keys
        keysout.clear();
        types.get(0).decompress(desc, inbuff, keysout);
        if (headout == null)
            // no need to decode the rest if we aren't reconstructing headers
            return;
        headout.clear();

        // row metadata
        BoundedBitSet metaparent = BoundedBitSet.from(LongType.decode(inbuff));
        long[] markedForDeleteAt = LongType.decode(inbuff);
        long[] localDeletionTime = LongType.decode(inbuff);
        // the amount of metadata should equal the total number of increments less the
        // number of parent changes: the first parent bit per parent represents a null
        assert markedForDeleteAt.length == (metaparent.index() - keysout.size()) :
            markedForDeleteAt.length + " vs (" + metaparent.index() + " - " + keysout.size() + ")";

        // name level
        ArrayList<ByteBuffer> names = new ArrayList<ByteBuffer>();
        BoundedBitSet nameparent = BoundedBitSet.from(LongType.decode(inbuff));
        types.get(1).decompress(desc, inbuff, names);
        // the number of names should equal the total number of increments less the
        // number of parent changes: the first parent bit per parent represents a null
        assert names.size() == (nameparent.index() - keysout.size()) :
            names.size() + " vs (" + nameparent.index() + " - " + keysout.size() + ")";

        // decode the offsets using the LongType primitive
        long[] offsets = LongType.decode(inbuff);
        

        
        /*
        System.out.println("block " + from);
        for (Collection<ByteBuffer> nlist : Arrays.asList(keysout, names))
        {
            ArrayList<String> stringz = new ArrayList<String>();
            for (ByteBuffer name : nlist)
                // assuming strings
                stringz.add(ByteBufferUtil.string(name));
            System.out.println("\t" + stringz);
        }
        System.out.println("\t" + Arrays.toString(offsets));
        System.out.println("\tm " + Arrays.toString(markedForDeleteAt));
        System.out.println("\tl " + Arrays.toString(localDeletionTime));
        */



        /**
         * reconstruct positions using the keys, parent flags and offsets: runs of equal parent
         * bits are entries for a particular parent. example: 1 consecutive parent bit represents a
         * narrow row, where we didn't bother to record children or metadata. 2 consecutive bits
         * represents a parent with exactly one child and 3 or more bits represent 2 or more
         * children. A simple BlockHeader will be created for 1 bit: a NestedBlockHeader will be created
         * for 2 or more bits.
         */
        int keyidx = 0, metaidx = 0, nameidx = 0;
        int offsetstart = 0, metastart = 0;
        while (offsetstart < nameparent.index() && metastart < metaparent.index())
        {
            int offsetend = (int)nameparent.endOfRun(offsetstart);
            int metaend = (int)metaparent.endOfRun(metastart);
            int namerunlength = offsetend - offsetstart;
            int metarunlength = metaend - metastart;
            // System.out.println("Adding: nl=" + namerunlength + " ml=" + metarunlength + " o=" + offsetstart + " m=" + metaidx);
            if (namerunlength == 1 && metarunlength == 1)
                // narrow/unobserved row: one offset, no metadata, no names
                headout.add(Collections.<BlockHeader>singletonList(new BlockHeader(offsets[offsetstart])));
            else if (metarunlength > 1 && namerunlength == 1)
            {
                // tombstone row: metadata, but no columns
                headout.add(Collections.<BlockHeader>singletonList(new NestedBlockHeader(offsets[offsetstart],
                                                                                         markedForDeleteAt[metaidx],
                                                                                         (int)localDeletionTime[metaidx],
                                                                                         null,
                                                                                         null)));
                metaidx++;
            }
            else if (metarunlength > 1 && namerunlength == 2)
            {
                // row with a single name entry
                headout.add(Collections.<BlockHeader>singletonList(new NestedBlockHeader(offsets[offsetstart],
                                                                                         markedForDeleteAt[metaidx],
                                                                                         (int)localDeletionTime[metaidx],
                                                                                         names.get(nameidx),
                                                                                         names.get(nameidx))));
                nameidx++;
                metaidx++;
            }
            else
            {
                // FIXME: should flatten this into the outer loop
                assert metarunlength > 1 && namerunlength > 1 : metarunlength + ", " + namerunlength;
                // wide row: ignore the key offset, and use column offsets
                // the first column for the run is the minimum: the last is the max
                ArrayList<BlockHeader> forparent = new ArrayList<BlockHeader>(namerunlength);
                int fixme = nameidx + namerunlength - 1;
                // System.out.println("row from: " + nameidx + " (" + namerunlength + ")");
                ByteBuffer min = names.get(nameidx++);
                for (final int end = nameidx + namerunlength - 2; nameidx < end;)
                {
                    ByteBuffer max = names.get(nameidx++);
                    /*
                    System.out.println(String.format("\toff: %d of: %d min: %s max: %s",
                                                     nameidx - 1,
                                                     nameparent.index(),
                                                     types.get(1).getString(min),
                                                     types.get(1).getString(max)));
                    */
                    // FIXME: giving all blocks the same offset
                    forparent.add(new NestedBlockHeader(offsets[++offsetstart],
                                                        markedForDeleteAt[metaidx],
                                                        (int)localDeletionTime[metaidx],
                                                        min,
                                                        max));
                    min = max;
                }
                headout.add(forparent);
                // FIXME: remove
                assert nameidx == fixme : nameidx + " vs " + fixme;
                metaidx++;
            }
            // reset for next run
            offsetstart = offsetend;
            metastart = metaend;
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
