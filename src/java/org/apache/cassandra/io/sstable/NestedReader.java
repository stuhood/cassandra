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
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
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
                 ReplayPosition replayPosition,
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
        super(desc, components, metadata, replayPosition, partitioner, ifile, dfile, indexSummary, bloomFilter, maxDataAge, rowSizes, columnCounts);
        this.types = metadata.getTypes();
        this.decorator = new Decorator();
    }

    /**
     * Reads index blocks to return keys sequentially.
     * TODO: Using the system default partitioner: will not iterate secindexes
     */
    public static KeyIterator getKeyIterator(final Descriptor desc)
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
                        readBlock(desc, types, in, keys, null);
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
    protected void load(Set<DecoratedKey> keysToLoadInCache) throws IOException
    {
        loadBloomFilter();
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
            List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
            List<RowHeader> headers = new ArrayList<RowHeader>();
            // read from the index a block at a time
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                readBlock(input, keys, headers);

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
                    dbuilder.addPotentialBoundary(headers.get(i).position());
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
    protected RowHeader getPositionFromIndex(IndexSummary.KeyPosition sampledPosition, DecoratedKey decoratedKey, Operator op)
    {
        // read the sampled block from the on-disk index
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition.indexPosition, INDEX_FILE_BUFFER_BYTES);
        List<ByteBuffer> keybuff = new ArrayList<ByteBuffer>();
        List<RowHeader> headers = new ArrayList<RowHeader>();
        FileDataInput input = segments.next();
        try
        {
            // read a block: the key exists in this block, or not at all
            readBlock(input, keybuff, headers);
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

    private void readBlock(DataInput from, Collection<ByteBuffer> keysout, Collection<RowHeader> headout) throws IOException
    {
        readBlock(descriptor, types, from, keysout, headout);
    }

    /**
     * Read a block of keys and row headers from the index.
     * This method is the inverse of NestedIndexWriter.flush
     * @param keysout An output collection of row keys.
     * @param headout An output collection of headers corresponding to the keys, or null.
     */
    private static void readBlock(Descriptor desc,
                                  List<AbstractType> types,
                                  DataInput from,
                                  Collection<ByteBuffer> keysout,
                                  Collection<RowHeader> headout) throws IOException
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
        Observer.IndexedBitSet metaparent = Observer.IndexedBitSet.from(LongType.decode(inbuff));
        long[] markedForDeleteAt = LongType.decode(inbuff);
        long[] localDeletionTime = LongType.decode(inbuff);
        // the amount of metadata should equal the total number of increments less the
        // number of parent changes: the first parent bit per parent represents a null
        assert markedForDeleteAt.length == (metaparent.index - keysout.size()) :
            markedForDeleteAt.length + " vs (" + metaparent.index + " - " + keysout.size() + ")";

        // first name level
        ArrayList<ByteBuffer> names = new ArrayList<ByteBuffer>();
        Observer.IndexedBitSet nameparent = Observer.IndexedBitSet.from(LongType.decode(inbuff));
        types.get(1).decompress(desc, inbuff, names);
        // skip the remaining levels: when we allow random access to blocks, they will be useful
        for (int i = 2; i < types.size(); i++)
        {
            LongType.decode(inbuff);
            types.get(i).decompress(desc, inbuff, new ArrayList<ByteBuffer>());
        }
        // the number of names should equal the total number of increments less the
        // number of parent changes: the first parent bit per parent represents a null
        assert names.size() == (nameparent.index - keysout.size()) :
            names.size() + " vs (" + nameparent.index + " - " + keysout.size() + ")";

        // decode the offsets using the LongType primitive
        long[] offsets = LongType.decode(inbuff);
        
        /**
         * reconstruct positions using the keys, parent flags and offsets: runs of equal parent
         * bits are entries for a particular parent. example: 1 consecutive parent bit represents a
         * narrow row, where we didn't bother to record children or metadata. 2 consecutive bits
         * represents a parent with exactly one child and 3 or more bits represent 2 or more
         * children. A simple RowHeader will be created for 1 bit: a NestedRowHeader will be created
         * for 2 or more bits.
         */
        int keyidx = 0, metaidx = 0, nameidx = 0;
        int offsetstart = 0, metastart = 0;
        while (offsetstart < nameparent.index && metastart < metaparent.index)
        {
            int offsetend = (int)nameparent.endOfRun(offsetstart);
            int metaend = (int)metaparent.endOfRun(metastart);
            int namerunlength = offsetend - offsetstart;
            int metarunlength = offsetend - offsetstart;
            if (namerunlength == 1 && metarunlength == 1)
                // narrow/unobserved row: one offset, no metadata, no names
                headout.add(new RowHeader(offsets[offsetstart]));
            else if (metarunlength > 1 && namerunlength == 1)
            {
                // tombstone row: metadata, but no columns
                headout.add(new NestedRowHeader(offsets[offsetstart],
                                                markedForDeleteAt[metaidx],
                                                (int)localDeletionTime[metaidx],
                                                null,
                                                null));
                metaidx++;
            }
            else
            {
                assert metarunlength > 1 && namerunlength > 1 : metarunlength + ", " + namerunlength;
                // wide row: the first column for the run is the minimum: the last is the max
                ByteBuffer min = names.get(nameidx);
                ByteBuffer max = names.get(nameidx + namerunlength - 2);
                headout.add(new NestedRowHeader(offsets[offsetstart],
                                                markedForDeleteAt[metaidx],
                                                (int)localDeletionTime[metaidx],
                                                min,
                                                max));
                nameidx += namerunlength - 1;
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
