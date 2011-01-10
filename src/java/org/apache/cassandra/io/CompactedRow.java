package org.apache.cassandra.io;
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


import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;

public class CompactedRow extends AbstractIterator<IColumn> implements ICompactedRow, CloseableIterator<IColumn>
{
    private final DecoratedKey key;
    private final boolean shouldPurge;
    private ColumnFamily emptyColumnFamily;
    private final CompactionController controller;

    // a filtered iterator, and the closeableiterator it wraps
    private final MergeIterator<IColumn,IColumn> closeable;
    private final Iterator<IColumn> iterator;

    public CompactedRow(CompactionController controller, List<SSTableIdentityIterator> rows)
    {
        this.controller = controller;
        this.key = rows.get(0).getKey();
        this.shouldPurge = controller.shouldPurge(key);
        for (SSTableIdentityIterator row : rows)
        {
            if (emptyColumnFamily == null)
                emptyColumnFamily = row.getColumnFamily();
            else
                emptyColumnFamily.delete(row.getColumnFamily());
        }

        AbstractType type = emptyColumnFamily.getComparator();
        closeable = MergeIterator.get(rows, type.columnComparator, new Reducer());
        iterator = Iterators.filter(closeable, Predicates.notNull());
    }

    public IColumn computeNext()
    {
        if (!iterator.hasNext())
            return endOfData();
        return iterator.next();
    }

    public void close()
    {
        closeable.close();
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    /** @return The column family for this row, without any column content. */
    public ColumnFamily getEmptyColumnFamily()
    {
        return emptyColumnFamily;
    }

    /** Consumes this iterator to update the digest. */
    public void update(MessageDigest digest)
    {
        // no special-case for rows.size == 1, we're actually skipping some bytes here so just
        // blindly updating everything wouldn't be correct
        DataOutputBuffer out = new DataOutputBuffer();
        try
        {
            ColumnFamily.serializer().serializeCFInfo(emptyColumnFamily, out);
            digest.update(out.getData(), 0, out.getLength());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        while (this.hasNext())
        {
            this.next().updateDigest(digest);
        }
    }

    public boolean isEmpty()
    {
        boolean cfIrrelevant = ColumnFamilyStore.removeDeletedCF(emptyColumnFamily, controller.gcBefore) == null;
        return cfIrrelevant && !iterator.hasNext();
    }

    /**
     * Merges columns in sorted order
     * FIXME: churns through CSLM for the gc/resolution logic in addColumn.
     */
    private final class Reducer extends MergeIterator.Reducer<IColumn, IColumn>
    {
        ColumnFamily container = emptyColumnFamily.cloneMeShallow();

        public void reduce(IColumn current)
        {
            container.addColumn(current);
        }

        protected IColumn getReduced()
        {
            // expecting 0 or 1 column
            ColumnFamily purged = shouldPurge ? ColumnFamilyStore.removeDeleted(container, controller.gcBefore) : container;
            if (purged != null && purged.metadata().getDefaultValidator().isCommutative())
            {
                CounterColumn.removeOldShards(purged, controller.gcBefore);
            }
            if (purged == null || purged.getEstimatedColumnCount() == 0)
            {
                container.clear();
                return null;
            }
            IColumn reduced = purged.iterator().next();
            container.clear();
            return reduced;
        }
    }
}
