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

package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.WrappedRunnable;
import  org.apache.thrift.TException;

import org.apache.cassandra.CassandraServiceController.Failure;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MutationTest extends TestBase
{
    @Test
    public void testInsert() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        client.set_keyspace(KEYSPACE);

        ByteBuffer key = ByteBuffer.wrap(String.format("test.key.%d", System.currentTimeMillis()).getBytes());

        insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.ONE);
        insert(client, key, "Standard1", "c2", "v2", 0, ConsistencyLevel.ONE);


        assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE));
        assertColumnEqual("c2", "v2", 0, getColumn(client, key, "Standard1", "c2", ConsistencyLevel.ONE));

        List<ColumnOrSuperColumn> coscs = get_slice(client, key, "Standard1", ConsistencyLevel.ONE);
        assertColumnEqual("c1", "v1", 0, coscs.get(0).column);
        assertColumnEqual("c2", "v2", 0, coscs.get(1).column);
    }

    @Test
    public void testQuorumInsertThenFailure() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        client.set_keyspace(KEYSPACE);

        ByteBuffer key = ByteBuffer.wrap(String.format("test.key.%d", System.currentTimeMillis()).getBytes());

        insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.QUORUM);
        insert(client, key, "Standard1", "c2", "v2", 0, ConsistencyLevel.QUORUM);


        Failure failure = controller.failHosts(hosts.get(0));
        try
        {
            // our original client connection is dead: open a new one
            client = controller.createClient(hosts.get(1));
            client.set_keyspace(KEYSPACE);

            assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.QUORUM));
            assertColumnEqual("c2", "v2", 0, getColumn(client, key, "Standard1", "c2", ConsistencyLevel.QUORUM));

            List<ColumnOrSuperColumn> coscs = get_slice(client, key, "Standard1", ConsistencyLevel.QUORUM);
            assertColumnEqual("c1", "v1", 0, coscs.get(0).column);
            assertColumnEqual("c2", "v2", 0, coscs.get(1).column);
        }
        finally
        {
            failure.resolve();
        }
    }

    protected void insert(Cassandra.Client client, ByteBuffer key, String cf, String name, String value, long timestamp, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        Column col = new Column(
             ByteBuffer.wrap(name.getBytes()),
             ByteBuffer.wrap(value.getBytes()),
             timestamp
             );
        client.insert(key, new ColumnParent(cf), col, cl);
    }

    protected Column getColumn(Cassandra.Client client, ByteBuffer key, String cf, String col, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException
    {
        ColumnPath cpath = new ColumnPath(cf);
        cpath.setColumn(col.getBytes());
        return client.get(key, cpath, cl).column;
    }

    protected List<ColumnOrSuperColumn> get_slice(Cassandra.Client client, ByteBuffer key, String cf, ConsistencyLevel cl)
      throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(
            new SliceRange(
                ByteBuffer.wrap(new byte[0]),
                ByteBuffer.wrap(new byte[0]),
                false,
                1000
                )
            );
        return client.get_slice(key, new ColumnParent(cf), sp, cl);
    }

    protected void assertColumnEqual(String name, String value, long timestamp, Column col)
    {
        assertEquals(ByteBuffer.wrap(name.getBytes()), col.name);
        assertEquals(ByteBuffer.wrap(value.getBytes()), col.value);
        assertEquals(timestamp, col.timestamp);
    }

}
