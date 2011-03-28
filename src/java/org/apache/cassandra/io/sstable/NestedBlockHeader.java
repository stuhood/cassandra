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
 * A summary of the metadata, position and min/max column names for a row. Very narrow
 * rows should use the BlockHeader structure (which does not hold metadata) instead.
 */
public class NestedBlockHeader extends BlockHeader
{
    private final long markedForDeleteAt;
    private final int localDeletionTime;
    
    private final ByteBuffer min;
    private final ByteBuffer max;

    NestedBlockHeader(long position, long markedForDeleteAt, int localDeletionTime, ByteBuffer min, ByteBuffer max)
    {
        super(position);
        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTime = localDeletionTime;
        this.min = min;
        this.max = max;
    }

    @Override
    public boolean isMetadataSet()
    {
        return true;
    }

    @Override
    public long markedForDeleteAt()
    {
        return markedForDeleteAt;
    }

    @Override
    public int localDeletionTime()
    {
        return localDeletionTime;
    }

    @Override
    public ByteBuffer min()
    {
        return min;
    }

    @Override
    public ByteBuffer max()
    {
        return max;
    }
}
