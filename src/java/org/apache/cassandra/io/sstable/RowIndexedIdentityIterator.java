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

class RowIndexedIdentityIterator extends SSTableIdentityIterator 
{
    private static final Logger logger = LoggerFactory.getLogger(RowIndexedIdentityIterator.class);

    private final DataInput input;
    private final DecoratedKey key;
    private final ColumnFamily columnFamily;
    private final int columnCount;
    private final long columnPosition;

    private final long dataStart;
    public final long dataSize;

    private BytesReadTracker inputWithTracker; // tracks bytes read

    private final boolean validateColumns;

    /**
     * Used to iterate through the columns of a row.
     * @param cfm The metadata for the column family for this sstable
     * @param partitioner
     * @param desc The descriptor for the data file we are reading from: this class does not assume that any of the other components of the sstable exist
     * @param file Reading using this file.
     * @param checkData if true, do its best to deserialize and check the coherence of row data
     * @param fromRemote True if this file arrived from another node
     * @throws IOException
     */
    RowIndexedIdentityIterator(CFMetaData metadata, IPartitioner partitioner, Descriptor descriptor, DataInput input, boolean checkData, boolean fromRemote)
    throws IOException
    {
        super(metadata, partitioner, descriptor, fromRemote);
        this.input = input;
        this.inputWithTracker = new BytesReadTracker(input);
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
                        IndexHelper.defreezeBloomFilter(file, dataSize, descriptor.version.usesOldBloomFilter);
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

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return columnFamily;
    }

    public long getDataSize()
    {
        return dataSize;
    }

    public boolean isBufferable(long limitBytes)
    {
        return dataSize <= limitBytes;
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

    public void echoData(DataOutput out) throws IOException
    {
        if (!(input instanceof RandomAccessReader))
            throw new UnsupportedOperationException();
        ((RandomAccessReader) input).seek(dataStart);

        assert dataSize > 0;
        out.writeLong(dataSize);
        inputWithTracker.reset(0);
        while (inputWithTracker.getBytesRead() < dataSize)
            out.write(inputWithTracker.readByte());
    }

    public ColumnFamily getColumnFamilyWithColumns() throws IOException
    {
        assert inputWithTracker.getBytesRead() == headerSize();
        ColumnFamily cf = columnFamily.cloneMeShallow();
        // since we already read column count, just pass that value and continue deserialization
        ColumnFamily.serializer().deserializeColumns(inputWithTracker, cf, columnCount, fromRemote);
        if (validateColumns)
        {
            try
            {
                cf.validateColumnFields();
            }
            catch (MarshalException e)
            {
                throw new IOException("Error validating row " + key, e);
            }
        }
        return cf;
    }

    private long headerSize()
    {
        return columnPosition - dataStart;
    }

    public int getColumnCount()
    {
        return columnCount;
    }

    public int getEstimatedColumnCount()
    {
        return columnCount;
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

    public void close() throws IOException
    {
        // creator is responsible for closing file when finished: but skip the content
        FileUtils.skipBytesFully(inputWithTracker, dataSize - inputWithTracker.getBytesRead());
    }
}
