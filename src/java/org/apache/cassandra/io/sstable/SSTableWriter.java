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
        iwriter = new IndexWriter(descriptor, partitioner, replayPosition, keyCount);
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

    private void afterAppend(DecoratedKey decoratedKey, long dataPosition, long dataSize, long columnCount) throws IOException
    {
        lastWrittenKey = decoratedKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        iwriter.afterAppend(decoratedKey, dataPosition, dataSize, columnCount);
        dbuilder.addPotentialBoundary(dataPosition);
    }

    public long append(AbstractCompactedRow row) throws IOException
    {
        long currentPosition = beforeAppend(row.key);
        ByteBufferUtil.writeWithShortLength(row.key.key, dataFile);
        row.write(dataFile);
        afterAppend(row.key, currentPosition, dataFile.getFilePointer() - currentPosition, row.columnCount());
        return currentPosition;
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile);
        // write placeholder for the row size, since we don't know it yet
        long sizePosition = dataFile.getFilePointer();
        dataFile.writeLong(-1);
        // write out row data
        int columnCount = ColumnFamily.serializer().serializeWithIndexes(cf, dataFile);
        // seek back and write the row size (not including the size Long itself)
        long endPosition = dataFile.getFilePointer();
        dataFile.seek(sizePosition);
        long dataSize = endPosition - (sizePosition + 8);
        assert dataSize > 0;
        dataFile.writeLong(dataSize);
        // finally, reset for next row
        dataFile.seek(endPosition);
        afterAppend(decoratedKey, startPosition, endPosition - startPosition, columnCount);
    }

    /** TODO: Appending with this method will result in an inaccurate column count. */
    public void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile);
        assert value.remaining() > 0;
        dataFile.writeLong(value.remaining());
        ByteBufferUtil.write(value, dataFile);
        // see method javadoc
        afterAppend(decoratedKey, currentPosition, value.remaining(), 0);
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
