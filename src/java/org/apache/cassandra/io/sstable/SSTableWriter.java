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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableWriter extends SSTable
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private IndexWriter iwriter;
    // collects positions of tuples within rows
    private Observer rowObserver;
    private SegmentedFile.Builder dbuilder;
    private final BufferedRandomAccessFile dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;

    public SSTableWriter(String filename, long keyCount) throws IOException
    {
        this(filename, keyCount, DatabaseDescriptor.getCFMetaData(Descriptor.fromFilename(filename)), StorageService.getPartitioner(), ReplayPosition.NONE);
    }

    public SSTableWriter(String filename, long keyCount, CFMetaData metadata, IPartitioner partitioner, ReplayPosition replayPosition) throws IOException
    {
        super(Descriptor.fromFilename(filename),
              new HashSet<Component>(Arrays.asList(Component.DATA, Component.FILTER, Component.PRIMARY_INDEX, Component.STATS)),
              metadata,
              replayPosition,
              partitioner);
        iwriter = IndexWriter.create(descriptor, metadata, partitioner, replayPosition, keyCount);
        rowObserver = iwriter.observer();
        dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        dataFile = new BufferedRandomAccessFile(new File(getFilename()), "rw", BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE, true);
    }
    
    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void reset()
    {
        try
        {
            dataFile.reset(dataMark);
            iwriter.reset();
            rowObserver.reset(false);
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
        iwriter.append(rowObserver, dataSize);
        dbuilder.addPotentialBoundary(startPosition);
        lastWrittenKey = key;
    }

    /** Appends a compacted row and optionally computes a header/cache entry. */
    public RowHeader append(AbstractCompactedRow row, boolean computeHeader) throws IOException
    {
        long startPosition = beforeAppend(row.key);
        rowObserver.add(row.key, startPosition);
        ByteBufferUtil.writeWithShortLength(row.key.key, dataFile);
        row.write(dataFile, rowObserver);
        RowHeader header = null;
        if (computeHeader)
        {
            // use the observed data to create a row header
            List<ByteBuffer> names = rowObserver.names.get(0);
            if (names.size() < 3)
                // observer didn't create a complex entry
                header = new RowHeader(startPosition);
            else
            {
                // row was wide enough for a nested entry
                ColumnFamily meta = row.getMetadata();
                // the min and max top level column are guaranteed to be observed
                ByteBuffer min = names.get(0);
                ByteBuffer max = names.get(names.size() - 1);
                header = new NestedRowHeader(startPosition,
                                             meta.getMarkedForDeleteAt(),
                                             meta.getLocalDeletionTime(),
                                             min,
                                             max);
            }
        }
        afterAppend(row.key, startPosition);
        return header;
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        rowObserver.add(decoratedKey, startPosition);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile);
        if (cf.serializedSize() < DatabaseDescriptor.getColumnIndexSize())
            // serialize with a simple index entry
            ColumnFamily.serializer().serializeForSSTable(cf, dataFile, Observer.NOOP);
        else
        {
            // serialize with a complex index entry: metadata and column blocks
            rowObserver.add(cf);
            ColumnFamily.serializer().serializeForSSTable(cf, dataFile, rowObserver);
        }
        afterAppend(decoratedKey, startPosition);
    }

    /**
     * Appends data without deserializing, leading to a basic index entry which can't
     * be used to eliminate the row at query time.
     */
    public void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        rowObserver.add(decoratedKey, startPosition);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile);
        assert value.remaining() > 0;
        dataFile.writeLong(value.remaining());
        ByteBufferUtil.write(value, dataFile);
        afterAppend(decoratedKey, startPosition);
    }

    public SSTableReader closeAndOpenReader() throws IOException
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge) throws IOException
    {
        // index, filter and statistics
        iwriter.close();

        // main data
        long position = dataFile.getFilePointer();
        dataFile.close(); // calls force
        FileUtils.truncate(dataFile.getPath(), position);

        // remove the 'tmp' marker from all components
        final Descriptor newdesc = rename(descriptor, components);

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(newdesc.filenameFor(SSTable.COMPONENT_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_DATA));
        SSTableReader sstable = SSTableReader.internalOpen(newdesc, components, metadata, replayPosition, partitioner, ifile, dfile, iwriter.summary, iwriter.bf, maxDataAge, iwriter.estimatedRowSize, iwriter.estimatedColumnCount);
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
}
