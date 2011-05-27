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

import java.security.MessageDigest;

import org.apache.cassandra.db.DecoratedKey;

/**
 * a CompactedRow is an object that takes a bunch of rows (keys + columnfamilies)
 * and can write iterate over a compacted version of those rows.  It does
 * NOT necessarily require creating a merged CF object in memory.
 */
public interface ICompactedRow
{
    public abstract DecoratedKey getKey();

    /**
     * update @param digest with the data bytes of the row (not including row key or row size)
     */
    public abstract void update(MessageDigest digest);

    /**
     * @return true if there are no columns in the row AND there are no row-level tombstones to be preserved
     */
    public abstract boolean isEmpty();
}
