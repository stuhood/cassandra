package org.apache.cassandra.db;
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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.ICompactSerializer3;
import org.apache.cassandra.io.sstable.Observer;

public class ColumnFamilySerializer implements ICompactSerializer3<ColumnFamily>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilySerializer.class);

    /*
     * Serialized ColumnFamily format:
     *
     * [serialized for intra-node writes only, e.g. returning a query result]
     * <cf nullability boolean: false if the cf is null>
     * <cf id>
     *
     * [in sstable only]
     * <column bloom filter>
     * <sparse column index, start/finish columns every ColumnIndexSizeInKB of data>
     *
     * [always present]
     * <local deletion time>
     * <client-provided deletion time>
     * <column count>
     * <columns, serialized individually>
    */
    public void serialize(ColumnFamily columnFamily, DataOutput dos)
    {
        try
        {
            if (columnFamily == null)
            {
                dos.writeBoolean(false);
                return;
            }

            dos.writeBoolean(true);
            dos.writeInt(columnFamily.id());
            serializeCFInfo(columnFamily, dos);

            Collection<IColumn> columns = columnFamily.getSortedColumns();
            dos.writeInt(columns.size());
            for (IColumn column : columns)
                columnFamily.getColumnSerializer().serialize(column, dos);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void serializeForSSTable(ColumnFamily cf, RandomAccessFile out, Observer observer) throws IOException
    {
        // write placeholder for the row size, since we don't know it yet
        long sizePosition = out.getFilePointer();
        out.writeLong(-1);
        // write out the row index, and then the row content
        ColumnIndexer.serialize(cf, out);
        serializeContentForSSTable(cf, out, observer);
        // seek back and write the row size (not including the size Long itself)
        long endPosition = out.getFilePointer();
        out.seek(sizePosition);
        long dataSize = endPosition - (sizePosition + 8);
        assert dataSize > 0;
        out.writeLong(dataSize);
        // finally, reset for next row
        out.seek(endPosition);
    }

    private void serializeContentForSSTable(ColumnFamily cf, RandomAccessFile dos, Observer observer) throws IOException
    {
        serializeCFInfo(cf, dos);
        Collection<IColumn> columns = cf.getSortedColumns();
        dos.writeInt(columns.size());
        for (Iterator<IColumn> iter = columns.iterator(); iter.hasNext();)
        {
            IColumn column = iter.next();
            long offset = dos.getFilePointer();
            cf.getColumnSerializer().serialize(column, dos);
            if (observer.shouldAdd(1, !iter.hasNext()))
                observer.add(1, column.name(), offset);
            observer.increment(dos.getFilePointer() - offset);
        }
    }

    public void serializeCFInfo(ColumnFamily columnFamily, DataOutput dos) throws IOException
    {
        dos.writeInt(columnFamily.localDeletionTime.get());
        dos.writeLong(columnFamily.markedForDeleteAt.get());
    }

    public ColumnFamily deserialize(DataInput dis) throws IOException
    {
        return deserialize(dis, false, false);
    }

    public ColumnFamily deserialize(DataInput dis, boolean intern, boolean fromRemote) throws IOException
    {
        if (!dis.readBoolean())
            return null;

        // create a ColumnFamily based on the cf id
        int cfId = dis.readInt();
        if (CFMetaData.getCF(cfId) == null)
            throw new UnserializableColumnFamilyException("Couldn't find cfId=" + cfId, cfId);
        ColumnFamily cf = ColumnFamily.create(cfId);
        deserializeFromSSTableNoColumns(cf, dis);
        deserializeColumns(dis, cf, intern, fromRemote);
        return cf;
    }

    public void deserializeColumns(DataInput dis, ColumnFamily cf, boolean intern, boolean fromRemote) throws IOException
    {
        int size = dis.readInt();
        int expireBefore = (int) (System.currentTimeMillis() / 1000);
        ColumnFamilyStore interner = intern ? Table.open(CFMetaData.getCF(cf.id()).left).getColumnFamilyStore(cf.id()) : null;
        for (int i = 0; i < size; ++i)
        {
            IColumn column = cf.getColumnSerializer().deserialize(dis, interner, fromRemote, expireBefore);
            cf.addColumn(column);
        }
    }

    /**
     * Observes columns in a single row, without adding them to the column family.
     */
    public void observeColumnsInSSTable(DecoratedKey key, ColumnFamily cf, RandomAccessFile dis, Observer observer) throws IOException
    {
        int size = dis.readInt();
        int expireBefore = (int) (System.currentTimeMillis() / 1000);
        for (int i = 0; i < size; ++i)
        {
            long offset = dis.getFilePointer();
            IColumn column = cf.getColumnSerializer().deserialize(dis, null, false, expireBefore);
            if (observer.shouldAdd(1, i == size))
                observer.add(1, column.name(), offset);
            observer.increment(dis.getFilePointer() - offset);
        }
    }

    public ColumnFamily deserializeFromSSTableNoColumns(ColumnFamily cf, DataInput input) throws IOException
    {
        cf.delete(input.readInt(), input.readLong());
        return cf;
    }

    public long serializedSize(ColumnFamily cf)
    {
        return cf.serializedSize();
    }
}
