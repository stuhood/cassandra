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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.Pair;

/**
 * This is a simple container for a key and its corresponding position
 * in a file. Binary search is performed on a list of these objects
 * to find where to start looking for the index entry containing the data position
 */
public final class Position implements Comparable<Position>
{
    public final DecoratedKey key;
    public final long position;

    Position(DecoratedKey key, long position)
    {
        this.key = key;
        this.position = position;
    }

    public int compareTo(Position kp)
    {
        return key.compareTo(kp.key);
    }

    public String toString()
    {
        return key + ":" + position;
    }

    /**
     * @return The offset in positions of the first key less than or equal to dk, or -1.
     */
    public static int binarySearch(List<Position> positions, DecoratedKey dk)
    {
        int index = Collections.binarySearch(positions, new Position(dk, -1));
        if (index >= 0)
            // exact match
            return index;
        // binary search gives us the first index _greater_ than the key searched for,
        // i.e., its insertion position
        int greaterThan = (index + 1) * -1;
        if (greaterThan == 0)
            return -1;
        return greaterThan - 1;
    }
}
