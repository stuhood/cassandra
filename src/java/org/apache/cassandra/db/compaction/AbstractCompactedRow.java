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

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.IIterableColumns;

/**
 * a CompactedRow is an object that takes a bunch of rows (keys + columnfamilies)
 * and can write a compacted version of those rows to an output stream.  It does
 * NOT necessarily require creating a merged CF object in memory.
 *
 * The two paths for consuming a CompactedRow are:
 * 1) CompactedRow.write()
 *    Appends a serialized blob of data to an output (possibly by performing 2 read passes)
 * 2) CompactedRow.iterator()
 *    Allows iteration over the merged content of the row
 */
public abstract class AbstractCompactedRow implements IIterableColumns
{
    public final DecoratedKey key;

    public AbstractCompactedRow(DecoratedKey key)
    {
        this.key = key;
    }

    @Override
    public AbstractType getComparator()
    {
        return getMetadata().getComparator();
    }

    /** @return The column family for this row, without any column content. */
    public abstract ColumnFamily getMetadata();

    /**
     * @return The full column family for this row, or null if it is too large to
     * fit in memory.
     */
    public abstract ColumnFamily getFullColumnFamily();

    /**
     * write the row (size + column index + filter + column data, but NOT row key) to @param out
     */
    public abstract long write(DataOutput out) throws IOException;

    /**
     * update @param digest with the data bytes of the row (not including row key or row size)
     */
    public abstract void update(MessageDigest digest);

    /**
     * @return true if there are no columns in the row AND there are no row-level tombstones to be preserved
     */
    public abstract boolean isEmpty();

    /**
     * @return the number of columns in the row
     */
    public abstract int columnCount();

    /**
     * @return the max column timestamp in the row
     */
    public abstract long maxTimestamp();
}
