/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.streaming;

/**
 * Streaming operation type.
 */
public enum OperationType
{
    // TODO: the only types of operation that are currently distinguised are AES and everything else.  There is no
    // sense in having the other types (yet).
    AES,
    BOOTSTRAP,
    UNBOOTSTRAP,
    RESTORE_REPLICA_COUNT,
    // FIXME: this enum is used for both streaming and sstable rebuilding
    ADD_INDEX;
}

