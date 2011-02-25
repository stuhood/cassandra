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
package org.apache.cassandra.db.marshal;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;

public class CounterColumnType extends AbstractCommutativeType
{
    public static final CounterColumnType instance = new CounterColumnType();

    CounterColumnType() {} // singleton

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1.remaining() == 0)
        {
            return o2.remaining() == 0 ? 0 : -1;
        }
        if (o2.remaining() == 0)
        {
            return 1;
        }

        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    public String getString(ByteBuffer bytes)
    {
        if (bytes.remaining() == 0)
        {
            return "";
        }
        if (bytes.remaining() != 8)
        {
            throw new MarshalException("A long is exactly 8 bytes");
        }
        return String.valueOf(bytes.getLong(bytes.position()));
    }

    /**
     * create commutative column
     */
    public Column createColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        return new CounterUpdateColumn(name, value, timestamp);
    }

    /**
     * remove target node from commutative columns
     */
    public void cleanContext(IColumnContainer cc, InetAddress node)
    {
        if ((cc instanceof ColumnFamily) && ((ColumnFamily)cc).isSuper())
        {
            for (IColumn column : cc.getSortedColumns())
            {
                SuperColumn supercol = (SuperColumn)column;
                cleanContext(supercol, node);
                if (0 == supercol.getSubColumns().size())
                    cc.remove(supercol.name());
            }
            return;
        }

        for (IColumn column : cc.getSortedColumns())
        {
            if (!(column instanceof CounterColumn)) // DeletedColumn
                continue;
            CounterColumn counterColumn = (CounterColumn)column;
            CounterColumn cleanedColumn = counterColumn.cleanNodeCounts(node);
            if (cleanedColumn == counterColumn)
                continue;
            cc.remove(counterColumn.name());
            //XXX: on "clean," must copy-and-replace
            if (null != cleanedColumn)
                cc.addColumn(cleanedColumn, HeapAllocator.instance);
        }
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte long (%d)", bytes.remaining()));
    }
}

