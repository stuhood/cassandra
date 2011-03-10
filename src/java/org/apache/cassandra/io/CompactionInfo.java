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

import java.io.Serializable;

/** Implements serializable to allow structured info to be returned via JMX. */
public final class CompactionInfo implements Serializable
{
    private final String ksname;
    private final String cfname;
    private final String tasktype;
    private final long bytesComplete;
    private final long totalBytes;

    public CompactionInfo(String ksname, String cfname, String tasktype, long bytesComplete, long totalBytes)
    {
        this.ksname = ksname;
        this.cfname = cfname;
        this.tasktype = tasktype;
        this.bytesComplete = bytesComplete;
        this.totalBytes = totalBytes;
    }

    /** @return A copy of this CompactionInfo with updated progress. */
    public CompactionInfo forProgress(long bytesComplete, long totalBytes)
    {
        return new CompactionInfo(ksname, cfname, tasktype, bytesComplete, totalBytes);
    }

    public String getKeyspace()
    {
        return ksname;
    }

    public String getColumnFamily()
    {
        return cfname;
    }

    public long getBytesComplete()
    {
        return bytesComplete;
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public String getTaskType()
    {
        return tasktype;
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(getTaskType()).append('@').append(hashCode());
        buff.append('(').append(getKeyspace()).append(", ").append(getColumnFamily());
        buff.append(", ").append(getBytesComplete()).append('/').append(getTotalBytes());
        return buff.append(')').toString();
    }

    public interface Holder
    {
        public CompactionInfo getCompactionInfo();
    }
}
