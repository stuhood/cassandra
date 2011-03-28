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

import java.nio.ByteBuffer;

/**
 * The representation of a narrow row in memory: when possible, wider rows are summarized
 * using the NestedBlockHeader subclass, which contains row metadata and thresholds. This
 * base implementation is essentially just a boxed position.
 */
public class BlockHeader
{
    private final long position;

    BlockHeader(long position)
    {
        this.position = position;
    }

    /** @return The absolute position of the row in the data file. */
    public final long position()
    {
        return position;
    }

    /** @return True if this BlockHeader contains the metadata and min/max values for this row. */
    public boolean isMetadataSet()
    {
        return false;
    }

    public long markedForDeleteAt()
    {
        throw new UnsupportedOperationException();
    }

    public int localDeletionTime()
    {
        throw new UnsupportedOperationException();
    }

    /** @return The minimum column name in the row, or null if the row is empty. */
    public ByteBuffer min()
    {
        throw new UnsupportedOperationException();
    }

    /** @return The maximum column name in the row, or null if the row is empty. */
    public ByteBuffer max()
    {
        throw new UnsupportedOperationException();
    }

    public String toString()
    {
        return "BlockHeader(position=" +position + ",metadata=" + isMetadataSet() + ")";
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof BlockHeader))
            return false;
        BlockHeader that = (BlockHeader)o;
        return this.position() == that.position();
    }
}
