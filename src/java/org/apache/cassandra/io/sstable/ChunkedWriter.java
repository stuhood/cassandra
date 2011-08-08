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
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
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
        iwriter = new IndexWriter(descriptor, partitioner, keyCount);

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
        appender = new ChunkAppender(descriptor, metadata, dataFile, iwriter);
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
        long position = (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
        if (rowObserver.shouldAdd(0, false))
            rowObserver.add(decoratedKey, position);
        return position;
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

    public long append(AbstractCompactedRow row) throws IOException
    {
        throw new RuntimeException("FIXME: not implemented"); /*
        return append(row.key, row.getMetadata(), (Iterator<IColumn>)row, computeHeader);
        */
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        appender.append(decoratedKey, cf, iter);

        // TODO: needs to be recorded within the appender
        sstableMetadataCollector.updateMaxTimestamp(cf.maxTimestamp());
        sstableMetadataCollector.addRowSize(endPosition - startPosition);
        sstableMetadataCollector.addColumnCount(columnCount);

        afterAppend(decoratedKey, startPosition);
        return startPosition;
    }

    @Deprecated
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
        public final Descriptor desc;
        public final IPartitioner partitioner;
        public final SegmentedFile.Builder builder;
        public final IndexSummary summary;
        public final BloomFilter bf;
        private FileMark mark;

        protected DecoratedKey key = null;
        protected long dataPosition = -1;

        // state recorded between appends/resets
        protected long count = 0;

        IndexWriter(Descriptor desc, IPartitioner part, long keyCount) throws IOException
        {
            this.desc = desc;
            this.partitioner = part;
            indexFile = SequentialWriter.open(new File(desc.filenameFor(SSTable.COMPONENT_INDEX)), 8 * 1024 * 1024, true);
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
            summary = new IndexSummary(keyCount);
            bf = BloomFilter.getFilter(keyCount, 15);
            estimatedRowSize = SSTable.defaultRowHistogram();
            estimatedColumnCount = SSTable.defaultColumnHistogram();
        }

        /**
         * @return True if a tuple at the given depth should be added to the index.
         * By default, one key is collected per append/reset.
         */
        public boolean shouldAdd(int depth, boolean last)
        {
            return depth == 0 && !last;
        }

        /** Increments the count of tuples since last reset by the given value. */
        public void increment(long count)
        {
            this.count += count;
        }

        /** Adds a tuple at level 0: a row key. */
        public void add(DecoratedKey key, long position)
        {
            assert this.key == null : "Multiple keys appended per row?";
            this.key = key;
            this.dataPosition = position;
        }

        /**
         * Adds a column name that shouldAdd requested. If shouldAdd is overridden to
         * request column names, this method will need to be overridden to receive that data.
         */
        public void add(ByteBuffer name, long position) {}

        /** @return Entries recorded at the given level since last reset. */
        public List<ByteBuffer> getLevel(int level)
        {
            assert level == 0 : this.getClass() + " does not collect tuples at " + level;
            return Collections.singletonList(key.key);
        }

        /**
         * Appends the stored content for a row, and prepares for the next row.
         * TODO: Once we are using a block based data file format, the unit of observation
         * should be a block.
         */
        public void append(long dataSize) throws IOException
        {
            // assuming a single key/row per observer
            for (ByteBuffer key : getLevel(0))
                bf.add(key);
            estimatedRowSize.add(dataSize);
            estimatedColumnCount.add(count);
            // append and reset
            appendToIndex();
            reset();
        }

        /**
         * Appends an observer containing content for a row to the index file.
         */
        protected void appendToIndex() throws IOException
        {
            long indexPosition = indexFile.getFilePointer();
            ByteBufferUtil.writeWithShortLength(key.key, indexFile.stream);
            // write the position of the data to the index
            indexFile.stream.writeLong(dataPosition);
            if (logger.isTraceEnabled())
                logger.trace("wrote index of " + key + " at " + indexPosition);
            summary.maybeAddEntry(key, indexPosition);
            builder.addPotentialBoundary(indexPosition);

            key = null;
            dataPosition = -1;
        }

        /** Flushes the state of the index writer. */
        protected void flush() throws IOException {}

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        public void close() throws IOException
        {
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
            count = 0;
        }
        public String toString()
        {
            return "IndexWriter(" + desc + ")";
        }
    }
}
