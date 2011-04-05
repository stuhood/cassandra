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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.AbstractCompactedRow;
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
        this(filename, keyCount, DatabaseDescriptor.getCFMetaData(Descriptor.fromFilename(filename)), StorageService.getPartitioner());
    }

    public SSTableWriter(String filename, long keyCount, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        super(Descriptor.fromFilename(filename),
              new HashSet<Component>(Arrays.asList(Component.DATA, Component.FILTER, Component.PRIMARY_INDEX, Component.STATS)),
              metadata,
              partitioner);
        iwriter = IndexWriter.create(descriptor, metadata, partitioner, keyCount);
        rowObserver = iwriter.observer();
        dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        dataFile = new BufferedRandomAccessFile(new File(getFilename()), "rw", DatabaseDescriptor.getInMemoryCompactionLimit(), true);
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

    private long beforeAppend(DecoratedKey decoratedKey, ColumnFamily meta) throws IOException
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
            rowObserver.add(decoratedKey, meta, position);
        return position;
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
        long startPosition = beforeAppend(row.key, row.getMetadata());
        ByteBufferUtil.writeWithShortLength(row.key.key, dataFile);
        row.write(dataFile, rowObserver);
        RowHeader header = null;
        if (computeHeader)
        {
            // use the observed data to create a row header
            long dataSize = dataFile.getFilePointer() - startPosition;
            if (dataSize > DatabaseDescriptor.getColumnIndexSize())
            {
                // row is wide enough for a nested entry
                ColumnFamily meta = row.getMetadata();
                ByteBuffer min = null, max = null;
                if (!rowObserver.names.get(0).isEmpty())
                {
                    // the min and max top level column are guaranteed to be observed
                    // by NestedIndexWriter's Observer
                    List<ByteBuffer> n = rowObserver.names.get(0);
                    min = n.get(0);
                    max = n.get(n.size() - 1);
                }
                header = new NestedRowHeader(startPosition,
                                             meta.getMarkedForDeleteAt(),
                                             meta.getLocalDeletionTime(),
                                             min,
                                             max);
            }
            else
            {
                header = new RowHeader(startPosition);
            }
        }
        afterAppend(row.key, startPosition);
        return header;
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey, cf);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile);
        ColumnFamily.serializer().serializeForSSTable(cf, dataFile, rowObserver);
        afterAppend(decoratedKey, startPosition);
    }

    @Deprecated
    public void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException
    {
        assert false : "FIXME: Need to deserialize blob to index.";
        long startPosition = beforeAppend(decoratedKey, null);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile);
        assert value.remaining() > 0;
        dataFile.writeLong(value.remaining());
        ByteBufferUtil.write(value, dataFile);
        afterAppend(decoratedKey, startPosition);
        logger.warn("Appended " + decoratedKey + " as an unindexable blob.");
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
        SSTableReader sstable = SSTableReader.internalOpen(newdesc, components, metadata, partitioner, ifile, dfile, iwriter.summary, iwriter.bf, maxDataAge, iwriter.estimatedRowSize, iwriter.estimatedColumnCount);
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
