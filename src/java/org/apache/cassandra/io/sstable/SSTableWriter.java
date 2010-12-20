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

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.io.sstable.avro.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableWriter extends SSTable implements Closeable
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private DataFileWriter<Chunk> dataFile;
    private ChunkAppender appender;
    private DecoratedKey lastWrittenKey;

    private long mark = -1;

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
        dataFile = new DataFileWriter<Chunk>(new SpecificDatumWriter<Chunk>());
        dataFile.create(Chunk.SCHEMA$, new File(getFilename()));
        createAppender();
    }

    private void createAppender()
    {
        appender = new ChunkAppender(dataFile,
                                     metadata.cfType == ColumnFamilyType.Standard ? 3 : 4);
    }

    public void mark() throws IOException
    {
        assert appender.isFlushed() : "Cannot mark mid-row: call reset first";
        mark = dataFile.sync();
        iwriter.mark();
    }

    /** NB: This reopens the file at the mark, and should not be used lightly. */
    public void reset() throws IOException
    {
        assert mark != -1 : "Cannot reset without marking";
        // truncate and reopen
        dataFile.close();
        FileUtils.truncate(getFilename(), mark);
        dataFile = new DataFileWriter(new SpecificDatumWriter<Chunk>());
        dataFile = dataFile.appendTo(new File(getFilename()));
        // recreate the appender
        createAppender();
        iwriter.reset();
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
        return dataFile.sync();
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
        throw new RuntimeException("FIXME: not implemented"); /*
        long currentPosition = beforeAppend(row.key);
        ByteBufferUtil.writeWithShortLength(row.key.key, dataFile);
        row.write(dataFile);
        afterAppend(row.key, currentPosition, dataFile.getFilePointer() - currentPosition, row.columnCount());
        return currentPosition;
        */
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        int columnCount = appender.append(decoratedKey, cf, cf.getSortedColumns().iterator());
        afterAppend(decoratedKey, startPosition, dataFile.sync() - startPosition, columnCount);
    }

    /** TODO: Appending with this method will result in an inaccurate column count. */
    public void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException
    {
        // FIXME: used for BMT: needs to specify version so that we can _possibly_
        // continue to support this via Avro 'appendEncoded'
        throw new RuntimeException("FIXME: BMT support not implemented! Needs versioning.");
    }

    public void close() throws IOException
    {
        closeAndOpenReader(System.currentTimeMillis());
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
        appender.flush();
        dataFile.close();

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
        throw new RuntimeException("FIXME: not implemented");
        // return dataFile.getFilePointer();
    }
}
