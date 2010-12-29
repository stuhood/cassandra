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

        Column col1 = new Column(
            ByteBuffer.wrap("c1".getBytes()),
            ByteBuffer.wrap("v1".getBytes()),
            0
            );
        insert(client, key, "Standard1", col1, ConsistencyLevel.ONE);
        Column col2 = new Column(
            ByteBuffer.wrap("c2".getBytes()),
            ByteBuffer.wrap("v2".getBytes()),
            0
            );
        insert(client, key, "Standard1", col2, ConsistencyLevel.ONE);

        Thread.sleep(100);

        Column col3 = getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE);
        assertColumnEqual("c1", "v1", 0, col3);

        Column col4 = getColumn(client, key, "Standard1", "c2", ConsistencyLevel.ONE);
        assertColumnEqual("c2", "v2", 0, col4);

        List<ColumnOrSuperColumn> coscs = new LinkedList<ColumnOrSuperColumn>();
        List<ColumnOrSuperColumn> cocs2 = get_slice(client, key, "Standard1", ConsistencyLevel.ONE);
        coscs.add((new ColumnOrSuperColumn()).setColumn(col3));
        coscs.add((new ColumnOrSuperColumn()).setColumn(col2));
        assertColumnEqual("c1", "v1", 0, coscs2.get(0));
        assertColumnEqual("c1", "v1", 0, coscs2.get(1));
        assertEquals(
            coscs,
            coscs2
            );
    }

    protected void insert(Cassandra.Client client, ByteBuffer key, String cf, Column col, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
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

    @Test
    public void testQuorumInsertThenFailure() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        client.set_keyspace(KEYSPACE);

        ByteBuffer key = ByteBuffer.wrap(String.format("test.key.%d", System.currentTimeMillis()).getBytes());

        Column col1 = new Column(
            ByteBuffer.wrap("c1".getBytes()),
            ByteBuffer.wrap("v1".getBytes()),
            0
            );
        insert(client, key, "Standard1", col1, ConsistencyLevel.QUORUM);
        Column col2 = new Column(
            ByteBuffer.wrap("c2".getBytes()),
            ByteBuffer.wrap("v2".getBytes()),
            0
            );
        insert(client, key, "Standard1", col2, ConsistencyLevel.QUORUM);

        Thread.sleep(100);

        Failure failure = controller.failHosts(hosts.get(0));
        try
        {
            // our original client connection is dead: open a new one
            client = controller.createClient(hosts.get(1));
            client.set_keyspace(KEYSPACE);

            // verify get
            Column col3 = getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE);
            assertColumnEqual("c1", "v1", 0, col3);
            Column col4 = getColumn(client, key, "Standard1", "c2", ConsistencyLevel.ONE);
            assertColumnEqual("c2", "v2", 0, col4);


            // verify slice
            List<ColumnOrSuperColumn> coscs = new LinkedList<ColumnOrSuperColumn>();
            List<ColumnOrSuperColumn> coscs2 = get_slice(client, key, "Standard1", ConsistencyLevel.QUORUM);
            coscs.add((new ColumnOrSuperColumn()).setColumn(col3));
            coscs.add((new ColumnOrSuperColumn()).setColumn(col4));
            assertColumnEqual("c1", "v1", 0, cocs2.get(0));
            assertColumnEqual("c2", "v2", 0, cocs2.get(1));
            assertEquals(
                coscs,
                coscs2
                );
        }
        finally
        {
            failure.resolve();
        }
    }
}
