package org.apache.cassandra.db.compaction;
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
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.HeapAllocator;

/**
 * PrecompactedRow merges its rows in its constructor in memory.
 */
public class PrecompactedRow extends AbstractCompactedRow
{
    private static Logger logger = LoggerFactory.getLogger(PrecompactedRow.class);

    private final ColumnFamily compactedCf;
    private final int gcBefore;

    // For testing purposes
    public PrecompactedRow(DecoratedKey key, ColumnFamily compacted)
    {
        super(key);
        this.compactedCf = compacted;
        this.gcBefore = Integer.MAX_VALUE;
    }

    public static ColumnFamily removeDeletedAndOldShards(DecoratedKey key, CompactionController controller, ColumnFamily cf)
    {
        return removeDeletedAndOldShards(controller.shouldPurge(key), controller, cf);
    }

    public static ColumnFamily removeDeletedAndOldShards(boolean shouldPurge, CompactionController controller, ColumnFamily cf)
    {
        ColumnFamily compacted = shouldPurge ? ColumnFamilyStore.removeDeleted(cf, controller.gcBefore) : cf;
        if (shouldPurge && compacted != null && compacted.metadata().getDefaultValidator().isCommutative())
            CounterColumn.removeOldShards(compacted, controller.gcBefore);
        return compacted;
    }

    public PrecompactedRow(CompactionController controller, List<SSTableIdentityIterator> rows)
    {
        super(rows.get(0).getKey());
        gcBefore = controller.gcBefore;
        compactedCf = removeDeletedAndOldShards(this.key, controller, merge(this.key, rows));
    }

    private static ColumnFamily merge(DecoratedKey key, List<SSTableIdentityIterator> rows)
    {
        ColumnFamily cf = rows.get(0).getColumnFamily().cloneMeShallow();
        for (SSTableIdentityIterator row : rows)
            while (row.hasNext())
                cf.addColumn(row.next(), HeapAllocator.instance);
        return cf;
    }

    public Iterator<IColumn> iterator()
    {
        return compactedCf.getSortedColumns().iterator();
    }

    public long write(DataOutput out) throws IOException
    {
        assert compactedCf != null;
        DataOutputBuffer buffer = new DataOutputBuffer();
        DataOutputBuffer headerBuffer = new DataOutputBuffer();
        ColumnIndexer.serialize(compactedCf, headerBuffer);
        ColumnFamily.serializer().serializeForSSTable(compactedCf, buffer);
        int dataSize = headerBuffer.getLength() + buffer.getLength();
        out.writeLong(dataSize);
        out.write(headerBuffer.getData(), 0, headerBuffer.getLength());
        out.write(buffer.getData(), 0, buffer.getLength());
        return dataSize;
    }

    public void update(MessageDigest digest)
    {
        assert compactedCf != null;
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            ColumnFamily.serializer().serializeCFInfo(compactedCf, buffer);
            buffer.writeInt(compactedCf.getColumnCount());
            digest.update(buffer.getData(), 0, buffer.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        compactedCf.updateDigest(digest);
    }

    public boolean isEmpty()
    {
        return compactedCf == null || ColumnFamilyStore.removeDeletedCF(compactedCf, gcBefore) == null;
    }

    public int columnCount()
    {
        return compactedCf == null ? 0 : compactedCf.getColumnCount();
    }

    public long maxTimestamp()
    {
        return compactedCf.maxTimestamp();
    }

    public ColumnFamily getMetadata()
    {
        return compactedCf;
    }

    public ColumnFamily getFullColumnFamily()
    {
        return compactedCf;
    }

    public int getEstimatedColumnCount()
    {
        return columnCount();
    }
}
