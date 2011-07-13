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

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BoundedBitSet;
import org.apache.cassandra.utils.FBUtilities;

public class ChunkedWriter extends SSTableWriter
{
    private static Logger logger = LoggerFactory.getLogger(ChunkedWriter.class);

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final SequentialWriter dataFile;
    private ChunkAppender appender;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;
    private SSTableMetadata.Collector sstableMetadataCollector;

    public ChunkedWriter(Descriptor desc,
                         long keyCount,
                         CFMetaData metadata,
                         IPartitioner partitioner,
                         SSTableMetadata.Collector sstableMetadataCollector) throws IOException
    {
        super(desc,
              keyCount,
              metadata,
              partitioner,
              sstableMetadataCollector);
        iwriter = new IndexWriter(descriptor, metadata, partitioner, keyCount);

        if (compression)
        {
            logger.warn("Two forms of compression are enabled for " + descriptor);
            dbuilder = SegmentedFile.getCompressedBuilder();
            dataFile = CompressedSequentialWriter.open(getFilename(),
                                                       descriptor.filenameFor(Component.COMPRESSION_INFO),
                                                       true);
        }
        else
        {
            dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
            dataFile = SequentialWriter.open(new File(getFilename()), true);
        }

        this.sstableMetadataCollector = sstableMetadataCollector;
        createAppender();
    }

    private void createAppender()
    {
        appender = new ChunkAppender(descriptor,
                                     metadata,
                                     dataFile,
                                     iwriter,
                                     sstableMetadataCollector);
    }

    public void mark() throws IOException
    {
        dataMark = dataFile.mark();
        // iwriter buffers the row between appends: reset will clear it
    }

    public void resetAndTruncate()
    {
        try
        {
            dataFile.resetAndTruncate(dataMark);
            iwriter.reset();
            // recreate the appender: anything buffered for the current block will be lost
            createAppender();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private long beforeAppend(DecoratedKey decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + getFilename());
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey key, long startPosition) throws IOException
    {
        long dataSize = dataFile.getFilePointer() - startPosition;
        if (logger.isTraceEnabled())
            logger.trace("wrote row starting at " + startPosition);
        iwriter.append(dataSize);
        dbuilder.addPotentialBoundary(startPosition);
        lastWrittenKey = key;
    }

    public BlockHeader append(AbstractCompactedRow row, boolean computeHeader) throws IOException
    {
        return append(row.key, row.getMetadata(), row.iterator(), computeHeader);
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        append(decoratedKey, cf, cf.getSortedColumns().iterator(), false);
    }

    /**
     * @param decoratedKey Key to append
     * @param cf An empty CF representing the metadata for the row
     * @param iter Iterator over columns in the row
     * @param computeHeader True to compute and return a row header
     * @return The offset of the written row
     */
    private BlockHeader append(DecoratedKey decoratedKey, ColumnFamily cf, Iterator<IColumn> iter, boolean computeHeader) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        appender.append(decoratedKey, cf, iter);

        // max timestamp and column count are recorded within the appender
        sstableMetadataCollector.addRowSize(dataFile.getFilePointer() - startPosition);

        BlockHeader header = null;
        if (computeHeader)
        {
            // use the observed column names to create a row header
            List<ByteBuffer> names = iwriter.getLevel(1);
            if (names.size() < 3)
                // observer didn't create a complex entry
                header = new BlockHeader(startPosition);
            else
            {
                // the min and max top level column are guaranteed to be observed
                ByteBuffer min = names.get(0);
                ByteBuffer max = names.get(names.size() - 1);
                header = new NestedBlockHeader(startPosition,
                                             cf.getMarkedForDeleteAt(),
                                             cf.getLocalDeletionTime(),
                                             min,
                                             max);
            }
        }
        afterAppend(decoratedKey, startPosition);
        return header;
    }

    /**
     * Appends data without deserializing, leading to a basic index entry which can't
     * be used to eliminate the row at query time.
     */
    public void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException
    {
        // FIXME: used for BMT: needs to specify version so that we can _possibly_
        // continue to support this via Avro 'appendEncoded'
        throw new RuntimeException("FIXME: BMT support not implemented! Needs versioning.");
    }

    /**
     * Attempt to close the index writer and data file before deleting all temp components for the sstable
     */
    public void cleanupIfNecessary()
    {
        FileUtils.closeQuietly(iwriter);
        FileUtils.closeQuietly(dataFile);

        try
        {
            Set<Component> components = SSTable.componentsFor(descriptor, Descriptor.TempState.TEMP);
            if (!components.isEmpty())
                SSTable.delete(descriptor, components);
        }
        catch (Exception e)
        {
            logger.error(String.format("Failed deleting temp components for %s", descriptor), e);
        }
    }

    public SSTableReader closeAndOpenReader() throws IOException
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge) throws IOException
    {
        // index and filter
        iwriter.close();

        // main data, close will truncate if necessary
        appender.flush();
        dataFile.close(); // calls force

        // write sstable statistics
        SSTableMetadata sstableMetadata = sstableMetadataCollector.finalizeMetadata();
        writeMetadata(descriptor, sstableMetadata);

        // remove the 'tmp' marker from all components
        final Descriptor newdesc = rename(descriptor, components);

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(newdesc.filenameFor(SSTable.COMPONENT_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_DATA));
        SSTableReader sstable = SSTableReader.internalOpen(newdesc,
                                                           components,
                                                           metadata,
                                                           partitioner,
                                                           ifile,
                                                           dfile,
                                                           iwriter.summary,
                                                           iwriter.bf,
                                                           maxDataAge,
                                                           sstableMetadata);
        iwriter = null;
        dbuilder = null;
        return sstable;
    }

    static Descriptor rename(Descriptor tmpdesc, Set<Component> components)
    {
        Descriptor newdesc = tmpdesc.asTemporary(false);
        try
        {
            // do -Data last because -Data present should mean the sstable was completely renamed before crash
            for (Component component : Sets.difference(components, Collections.singleton(Component.DATA)))
                FBUtilities.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
            FBUtilities.renameWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return newdesc;
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    static class IndexWriter implements Closeable
    {
        private final SequentialWriter indexFile;
        // for each level, a type to be used for compression
        public final List<AbstractType> types;
        public final Descriptor desc;
        public final IPartitioner partitioner;
        public final SegmentedFile.Builder builder;
        public final IndexSummary summary;
        public final BloomFilter bf;
        private FileMark mark;

        // buffers for indexed content
        protected final List<ByteBuffer> keys;
        protected final List<ByteBuffer> names;
        // a bitset for the name level that toggles when the parent of that level has changed
        protected final BoundedBitSet nameparents;
        // TODO: avoid boxing
        protected final List<Long> offsets;
        // we store a single level's worth of metadata for observed rows
        protected final List<Long> markedForDeleteAt;
        protected final List<Long> localDeletionTime;
        protected final BoundedBitSet metaparent;
        // a reusable buffer for output data, before it is appended to the file
        private ByteBuffer outbuff = ByteBuffer.allocate(4096);
        protected final Observer observer;

        // last appended key and position
        protected DecoratedKey firstKey = null;
        protected long dataPosition = -1;

        public IndexWriter(Descriptor desc, CFMetaData meta, IPartitioner part, long keyCount) throws IOException
        {
            this.desc = desc;
            this.partitioner = part;
            indexFile = SequentialWriter.open(new File(desc.filenameFor(SSTable.COMPONENT_INDEX)), 8 * 1024 * 1024, true);
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
            summary = new IndexSummary(keyCount);
            bf = BloomFilter.getFilter(keyCount, 15);

            this.types = meta.getTypes();
            // the last type is for the values (not indexed)
            this.keys = new ArrayList<ByteBuffer>();
            this.names = new ArrayList<ByteBuffer>();
            this.nameparents = new BoundedBitSet(1024);
            this.offsets = new ArrayList<Long>();
            this.markedForDeleteAt = new ArrayList<Long>();
            this.localDeletionTime = new ArrayList<Long>();
            this.metaparent = new BoundedBitSet(1024);

            // TODO: should replace with local buffered/flushed offsets
            this.observer = new Observer(1024);
        }

        /** Adds a row key. */
        public void add(DecoratedKey key, long position)
        {
            this.firstKey = key;
            this.dataPosition = position;
            observer.add(key, position);
            System.out.println("++" + key + " at " + position);
        }

        /** Override to collect metadata at level 0: for a row. This impl drops it. */
        public void add(long markedForDeleteAt, int localDeletionTime)
        {
            observer.add(markedForDeleteAt, localDeletionTime);
        }

        /**
         * Adds a column name.
         */
        public void add(ByteBuffer name, long position)
        {
            observer.add(name, position);
            try
            {
                System.out.println("\t++" + ByteBufferUtil.string(name) + " at " + position);
            }
            catch (Exception e)
            {
                // pass
            }
        }

        /** @return Entries recorded at the given level since last reset. */
        public List<ByteBuffer> getLevel(int level)
        {
            if (level == 0)
                return observer.keys;
            assert level == 1;
            return observer.names;
        }

        /**
         * Appends the stored content for a row, and prepares for the next row.
         */
        public void append(long dataSize) throws IOException
        {
            // assuming a single key/row per observer
            for (ByteBuffer key : getLevel(0))
                bf.add(key);
            // append and reset
            appendToIndex();
            reset();
        }

        /**
         * IndexWriter buffers a block worth of index content before actually flushing:
         * this method copies from the per-row buffer (the observer) to the block buffer.
         */
        protected void appendToIndex() throws IOException
        {
            assert firstKey != null && observer.keys.size() == 1 : "Oddly sized row: " + observer;

            if (summary.shouldAddEntry())
            {
                // the summary would like to observe a value: flush the current block
                // so that the value to observe is the first in a new block
                this.flush();
                summary.addEntry(firstKey, indexFile.getFilePointer());
                if (logger.isTraceEnabled())
                    logger.trace("recorded summary of index for " + firstKey + " at " + indexFile.getFilePointer());
            }
            summary.incrementRowid();

            // copy observed content into our storage
            this.keys.addAll(observer.keys);
            this.names.addAll(observer.names);
            this.nameparents.copyFrom(observer.nameparents);
            this.offsets.addAll(observer.offsets);
            this.markedForDeleteAt.addAll(observer.markedForDeleteAt);
            this.localDeletionTime.addAll(observer.localDeletionTime);
            this.metaparent.copyFrom(observer.metaparent);

            // reset to collect the next row
            observer.reset(true);
            firstKey = null;
        }

        /** Closes the current block: this method is the inverse of NestedReader.readBlock. */
        protected void flush() throws IOException
        {
            if (offsets.isEmpty())
                return;
            // prepare the buffer for reuse
            outbuff.clear();

            // mark the beginning of the block as a splittable position
            long start = indexFile.getFilePointer();
            builder.addPotentialBoundary(start);

            // encode keys using the appropriate AbstractType
            outbuff = types.get(0).compress(desc, keys, outbuff);
            // record metadata (0 or 1 entry per key, depending on row width)
            outbuff = LongType.encode(metaparent.asLongCollection(), outbuff);
            outbuff = LongType.encode(new LongList(markedForDeleteAt), outbuff);
            outbuff = LongType.encode(new LongList(localDeletionTime), outbuff);
            // encode names
            outbuff = LongType.encode(nameparents.asLongCollection(), outbuff);
            outbuff = types.get(1).compress(desc, names, outbuff);
            // encode the offsets using the LongType primitive
            outbuff = LongType.encode(new LongList(offsets), outbuff);

            // append the buffer
            outbuff.flip();
            ByteBufferUtil.writeWithLength(outbuff, indexFile.stream);
            logger.debug("Wrote block of {} positions in {} bytes", offsets.size(), indexFile.getFilePointer() - start);
            clear();
        }

        /** Clears the content of the block buffer. */
        private void clear()
        {
            keys.clear();
            names.clear();
            nameparents.clear();
            metaparent.clear();
            offsets.clear();
            markedForDeleteAt.clear();
            localDeletionTime.clear();
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        public void close() throws IOException
        {
            flush();

            // bloom filter
            FileOutputStream fos = new FileOutputStream(desc.filenameFor(SSTable.COMPONENT_FILTER));
            DataOutputStream stream = new DataOutputStream(fos);
            BloomFilter.serializer().serialize(bf, stream);
            stream.flush();
            fos.getFD().sync();
            stream.close();

            // index
            long position = indexFile.getFilePointer();
            indexFile.close(); // calls force
            FileUtils.truncate(indexFile.getPath(), position);

            // finalize in-memory index state
            summary.complete();
        }

        public void mark()
        {
            mark = indexFile.mark();
        }

        public void resetAndTruncate() throws IOException
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            indexFile.resetAndTruncate(mark);
            reset();
        }

        /** Clear the stored state for the current row from the IndexWriter. */
        public void reset() throws IOException
        {
            observer.reset(false);
        }

        public String toString()
        {
            return "IndexWriter(" + desc + ")";
        }

        // TODO: the point of LongCollection was to avoid boxing: need
        // an implementation of LongList that doesn't box
        private static final class LongList extends LongType.LongCollection
        {
            private final List<Long> list;
            public LongList(List<Long> list)
            {
                super(list.size());
                this.list = list;
            }
            
            public long get(int i)
            {
                return list.get(i);
            }
        }
    }
}
