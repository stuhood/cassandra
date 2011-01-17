package org.apache.cassandra.io.util;
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


import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.db.IColumn;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Facade over an IColumn iterator that contains IColumns in sorted order.
 * We use this because passing a SortedMap to the ConcurrentSkipListMap constructor is the only way
 * to invoke its private buildFromSorted method and avoid worst-case behavior of CSLM.put.
 */
public class ColumnSortedMap extends TreeMap<ByteBuffer, IColumn>
{
    private final SortedSet<Map.Entry<ByteBuffer, IColumn>> entryset;

    public ColumnSortedMap(Comparator<ByteBuffer> comparator, Iterator<IColumn> iter)
    {
        super(comparator);
        this.entryset = new ColumnProjectingSet(iter);
    }

    @Override
    public int size()
    {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Set<Map.Entry<ByteBuffer, IColumn>> entrySet()
    {
        return entryset;
    }
}

/** Takes an iterator over columns, and projects the name and column into Map.Entry. */
class ColumnProjectingSet extends TreeSet<Map.Entry<ByteBuffer, IColumn>>
{
    private final Iterator<Entry<ByteBuffer, IColumn>> iter;

    public ColumnProjectingSet(Iterator<IColumn> iter)
    {
        super();
        this.iter = Iterators.<IColumn, Entry<ByteBuffer, IColumn>>transform(iter, new Function<IColumn, Entry<ByteBuffer, IColumn>>()
        {
            @Override
            public Entry<ByteBuffer, IColumn> apply(final IColumn column)
            {
                return new Entry<ByteBuffer, IColumn>()
                {
                    public IColumn setValue(IColumn value)
                    {
                        return null;
                    }

                    public IColumn getValue()
                    {
                        return column;
                    }

                    public ByteBuffer getKey()
                    {
                        return column.name();
                    }
                };
            }
        });
    }

    @Override
    public int size()
    {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Iterator<Entry<ByteBuffer, IColumn>> iterator()
    {
        return iter;
    }
}
