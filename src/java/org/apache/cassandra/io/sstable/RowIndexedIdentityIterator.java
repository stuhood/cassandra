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
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.ByteBufferUtil;

class RowIndexedIdentityIterator extends SSTableIdentityIterator 
{
    private static final Logger logger = LoggerFactory.getLogger(RowIndexedIdentityIterator.class);

    private final BufferedRandomAccessFile file;
    private final DecoratedKey key;
    private final ColumnFamily columnFamily;
    private final int columnCount;
    private final long columnPosition;

    private final long rowStart;
    private final long dataStart;
    public final long dataSize;
    private final long finishedAt;

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
    RowIndexedIdentityIterator(CFMetaData cfm, IPartitioner partitioner, Descriptor desc, BufferedRandomAccessFile file, boolean checkData, boolean fromRemote)
    throws IOException
    {
        super(cfm, partitioner, desc, fromRemote);
        this.file = file;
        this.validateColumns = checkData;

        try
        {
            this.rowStart = file.getFilePointer();
            this.key = SSTableReader.decodeKey(partitioner,
                                               desc,
                                               ByteBufferUtil.readWithShortLength(file));
            this.dataSize = SSTableReader.readRowSize(file, desc);
            if (dataSize < 0 || file.length() < rowStart + dataSize)
                throw new IOException("Impossible row size " + dataSize + " at " + rowStart);
            this.dataStart = file.getFilePointer();
            this.finishedAt = dataStart + dataSize;
            if (checkData)
            {
                try
                {
                    IndexHelper.defreezeBloomFilter(file, dataSize, desc.usesOldBloomFilter);
                }
                catch (Exception e)
                {
                    if (e instanceof EOFException)
                        throw (EOFException) e;

                    logger.debug("Invalid bloom filter in {}; will rebuild it", desc);
                    // deFreeze should have left the file position ready to deserialize index
                }
                try
                {
                    IndexHelper.deserializeIndex(file);
                }
                catch (Exception e)
                {
                    logger.debug("Invalid row summary in {}; will rebuild it", desc);
                }
                file.seek(this.dataStart);
            }
            IndexHelper.skipBloomFilter(file);
            IndexHelper.skipIndex(file);
            columnFamily = ColumnFamily.create(cfm);
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(columnFamily, file);
            columnCount = file.readInt();
            columnPosition = file.getFilePointer();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public String getPath()
    {
        return file.getPath();
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

    public boolean hasNext()
    {
        return file.getFilePointer() < finishedAt;
    }

    public IColumn next()
    {
        try
        {
            IColumn column = columnFamily.getColumnSerializer().deserialize(file, null, fromRemote, expireBefore);
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

    public final void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void echoData(DataOutput out) throws IOException
    {
        file.seek(dataStart);
        while (file.getFilePointer() < finishedAt)
        {
            out.write(file.readByte());
        }
    }

    public ColumnFamily getColumnFamilyWithColumns() throws IOException
    {
        file.seek(columnPosition - 4); // seek to before column count int
        ColumnFamily cf = columnFamily.cloneMeShallow();
        ColumnFamily.serializer().deserializeColumns(file, cf, false, fromRemote);
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

    public int getColumnCount()
    {
        return columnCount;
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return getKey().compareTo(o.getKey());
    }

    public void reset()
    {
        try
        {
            file.seek(columnPosition);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public long rowSize()
    {
        return finishedAt - rowStart;
    }
    
    public void close() throws IOException
    {
        // creator is responsible for closing file when finished: but skip the content
        file.seek(finishedAt);
    }
}
