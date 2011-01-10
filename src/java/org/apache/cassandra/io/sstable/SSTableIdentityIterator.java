package org.apache.cassandra.io.sstable;
/*
 * 
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
 * 
 */


import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BytesReadTracker;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, IColumnIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIdentityIterator.class);

    private final DecoratedKey key;
    private final DataInput input;
    private final long dataStart;
    public final long dataSize;
    public final boolean fromRemote;

    private final ColumnFamily columnFamily;
    public final int columnCount;
    private long columnPosition;

    private BytesReadTracker inputWithTracker; // tracks bytes read

    // Used by lazilyCompactedRow, so that we see the same things when deserializing the first and second time
    private final int expireBefore;

    private final boolean validateColumns;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param checkData if true, do its best to deserialize and check the coherence of row data
     * @throws IOException
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, boolean checkData)
    throws IOException
    {
        this(sstable.metadata, sstable.partitioner, sstable.descriptor, file, checkData, false);
    }

    public SSTableIdentityIterator(CFMetaData metadata, IPartitioner partitioner, Descriptor descriptor, DataInput file, boolean fromRemote)
    throws IOException
    {
        this(metadata, partitioner, descriptor, file, false, fromRemote);
    }

    // sstable may be null *if* deserializeRowHeader is false
    private SSTableIdentityIterator(CFMetaData metadata, IPartitioner partitioner, Descriptor descriptor, DataInput input, boolean checkData, boolean fromRemote)
    throws IOException
    {
        this.input = input;
        this.inputWithTracker = new BytesReadTracker(input);
        this.expireBefore = (int)(System.currentTimeMillis() / 1000);
        this.fromRemote = fromRemote;
        this.validateColumns = checkData;

        try
        {
            // read key and size without tracking: it is only applied to the content
            this.key = SSTableReader.decodeKey(partitioner,
                                               descriptor,
                                               ByteBufferUtil.readWithShortLength(input));
            this.dataSize = SSTableReader.readRowSize(input, descriptor);
            if (dataSize < 0)
                throw new IOException("Impossible row size " + dataSize + " for " + key);
            if (input instanceof RandomAccessReader)
            {
                RandomAccessReader file = (RandomAccessReader) input;
                this.dataStart = file.getFilePointer();
                if (dataStart + dataSize > file.length())
                    throw new IOException(String.format("dataSize of %s starting at %s would be larger than file %s length %s",
                                          dataSize, dataStart, file.getPath(), file.length()));
                if (checkData)
                {
                    try
                    {
                        IndexHelper.defreezeBloomFilter(file, dataSize, descriptor.usesOldBloomFilter);
                    }
                    catch (Exception e)
                    {
                        if (e instanceof EOFException)
                            throw (EOFException) e;

                        logger.debug("Invalid bloom filter for {} in {}; will rebuild it", key, descriptor);
                        // deFreeze should have left the file position ready to deserialize index
                    }
                    try
                    {
                        IndexHelper.deserializeIndex(file);
                    }
                    catch (Exception e)
                    {
                        logger.debug("Invalid row summary in {}; will rebuild it", descriptor);
                    }
                    file.seek(this.dataStart);
                    inputWithTracker.reset(0);
                }
            }
            else
            {
                // absolute positions not relevant in a stream
                this.dataStart = -1;
            }

            IndexHelper.skipBloomFilter(inputWithTracker);
            IndexHelper.skipIndex(inputWithTracker);
            columnFamily = ColumnFamily.create(metadata);
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(columnFamily, inputWithTracker);
            columnCount = inputWithTracker.readInt();

            columnPosition = dataStart + inputWithTracker.getBytesRead();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return columnFamily;
    }

    public boolean hasNext()
    {
        return inputWithTracker.getBytesRead() < dataSize;
    }

    public IColumn next()
    {
        try
        {
            IColumn column = columnFamily.getColumnSerializer().deserialize(inputWithTracker, fromRemote, expireBefore);
            if (validateColumns)
                column.validateFields(columnFamily.metadata());
            return column;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        catch (MarshalException e)
        {
            throw new IOError(new IOException("Error validating row " + key, e));
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public String getPath()
    {
        // if input is from file, then return that path, otherwise it's from streaming
        if (input instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) input;
            return file.getPath();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public void echoData(DataOutput out) throws IOException
    {
        if (!(input instanceof RandomAccessReader))
            throw new UnsupportedOperationException();

        ((RandomAccessReader) input).seek(dataStart);
        inputWithTracker.reset(0);
        while (inputWithTracker.getBytesRead() < dataSize)
            out.write(inputWithTracker.readByte());
    }


    private long headerSize()
    {
        return columnPosition - dataStart;
    }

    /**
     * @return True if the full content of the row could be buffered into a buffer
     * of the given size: valid before the iterator has been consumed.
     */
    public boolean isBufferable(long limitBytes)
    {
        return dataSize <= limitBytes;
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }

    public void reset()
    {
        if (!(input instanceof RandomAccessReader))
            throw new UnsupportedOperationException();

        RandomAccessReader file = (RandomAccessReader) input;
        try
        {
            file.seek(columnPosition);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        inputWithTracker.reset(headerSize());
    }

    /**
     * @return The width of the row.
     */
    public long getDataSize()
    {
        return dataSize;
    }

    /**
     * @return The count of top-level columns in the row.
     */
    public int getColumnCount()
    {
        return columnCount;
    }

    public void close() throws IOException
    {
        // creator is responsible for closing file when finished: but skip the content
        FileUtils.skipBytesFully(inputWithTracker, dataSize - inputWithTracker.getBytesRead());
    }
}
