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

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.*;

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
        final String keyspace = "TestInsert";
        addKeyspace(keyspace, 3);
        Cassandra.Client client = controller.createClient(hosts.get(0));
        client.set_keyspace(keyspace);

        ByteBuffer key = newKey();

        insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.ONE);
        insert(client, key, "Standard1", "c2", "v2", 0, ConsistencyLevel.ONE);


        assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE));
        assertColumnEqual("c2", "v2", 0, getColumn(client, key, "Standard1", "c2", ConsistencyLevel.ONE));

        List<ColumnOrSuperColumn> coscs = get_slice(client, key, "Standard1", ConsistencyLevel.ONE);
        assertColumnEqual("c1", "v1", 0, coscs.get(0).column);
        assertColumnEqual("c2", "v2", 0, coscs.get(1).column);
    }

    @Test
    public void testWriteAllReadOne() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        final String keyspace = "TestWriteAllReadOne";
        addKeyspace(keyspace, 3);
        client.set_keyspace(keyspace);

        ByteBuffer key = newKey();

        insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.ALL);
        assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE));

        List<InetAddress> endpoints = endpointsForKey(hosts.get(0), key, keyspace);
        InetAddress coordinator = nonEndpointForKey(hosts.get(0), key, keyspace);
        Failure failure = controller.failHosts(endpoints.subList(1, endpoints.size()));

        Thread.sleep(10000); // let gossip catch up

        try {
            client = controller.createClient(coordinator);
            client.set_keyspace(keyspace);

            assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ONE));

            insert(client, key, "Standard1", "c3", "v3", 0, ConsistencyLevel.ALL);
            assert false;
        } catch (UnavailableException e) {
            // [this is good]
        } finally {
            failure.resolve();
            Thread.sleep(10000);
        }
    }

    @Test
    public void testWriteQuorumReadQuorum() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        final String keyspace = "TestWriteQuorumReadQuorum";
        addKeyspace(keyspace, 3);
        client.set_keyspace(keyspace);

        ByteBuffer key = newKey();

        // with quorum-1 nodes up
        List<InetAddress> endpoints = endpointsForKey(hosts.get(0), key, keyspace);
        InetAddress coordinator = nonEndpointForKey(hosts.get(0), key, keyspace);
        Failure failure = controller.failHosts(endpoints.subList(1, endpoints.size())); //kill all but one nodes

        Thread.sleep(10000);
        client = controller.createClient(coordinator);
        client.set_keyspace(keyspace);
        try {
            insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.QUORUM);
            assert false;
        } catch (UnavailableException e) {
            // [this is good]
        } finally {
            failure.resolve();
            Thread.sleep(10000);
        }

        // with all nodes up
        insert(client, key, "Standard1", "c2", "v2", 0, ConsistencyLevel.QUORUM);

        failure = controller.failHosts(endpoints.get(0));
        Thread.sleep(10000);
        try {
            getColumn(client, key, "Standard1", "c2", ConsistencyLevel.QUORUM);
        } finally {
            failure.resolve();
            Thread.sleep(10000);
        }
    }

    @Test
    public void testWriteOneReadAll() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));

        final String keyspace = "TestWriteOneReadAll";
        addKeyspace(keyspace, 3);
        client.set_keyspace(keyspace);

        ByteBuffer key = newKey();

        List<InetAddress> endpoints = endpointsForKey(hosts.get(0), key, keyspace);
        InetAddress coordinator = nonEndpointForKey(hosts.get(0), key, keyspace);
        client = controller.createClient(coordinator);
        client.set_keyspace(keyspace);

        insert(client, key, "Standard1", "c1", "v1", 0, ConsistencyLevel.ONE);
        assertColumnEqual("c1", "v1", 0, getColumn(client, key, "Standard1", "c1", ConsistencyLevel.ALL));

        // with each of HH, read repair and proactive repair:
            // with one node up
            // write with one (success)
            // read with all (failure)
            // bring nodes up
            // repair
            // read with all (success)

        Failure failure = controller.failHosts(endpoints);
        Thread.sleep(10000);
        try {
            insert(client, key, "Standard1", "c2", "v2", 0, ConsistencyLevel.ONE);
            assert false;
        } catch (UnavailableException e) {
            // this is good
        } finally {
            failure.resolve();
        }
    }
}
