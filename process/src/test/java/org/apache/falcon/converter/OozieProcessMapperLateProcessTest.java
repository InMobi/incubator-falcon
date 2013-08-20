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
package org.apache.falcon.converter;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Test class for late data processing.
 */
public class OozieProcessMapperLateProcessTest {

    private static final String CLUSTER_XML = "/config/late/late-cluster.xml";
    private static final String FEED1_XML = "/config/late/late-feed1.xml";
    private static final String FEED2_XML = "/config/late/late-feed2.xml";
    private static final String FEED3_XML = "/config/late/late-feed3.xml";
    private static final String PROCESS1_XML = "/config/late/late-process1.xml";
    private static final String PROCESS2_XML = "/config/late/late-process2.xml";
    private static final ConfigurationStore STORE = ConfigurationStore.get();

    private static EmbeddedCluster dfsCluster;

    @BeforeClass
    public void setUpDFS() throws Exception {

        cleanupStore();

        dfsCluster = EmbeddedCluster.newCluster("testCluster");
        Configuration conf = dfsCluster.getConf();
        String hdfsUrl = conf.get("fs.default.name");

        Cluster cluster = (Cluster) EntityType.CLUSTER.getUnmarshaller()
                .unmarshal(this.getClass().getResource(CLUSTER_XML));
        ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(hdfsUrl);

        STORE.publish(EntityType.CLUSTER, cluster);

        Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                this.getClass().getResource(FEED1_XML));
        Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                this.getClass().getResource(FEED2_XML));
        Feed feed3 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                this.getClass().getResource(FEED3_XML));

        STORE.publish(EntityType.FEED, feed1);
        STORE.publish(EntityType.FEED, feed2);
        STORE.publish(EntityType.FEED, feed3);

        Process process1 = (Process) EntityType.PROCESS.getUnmarshaller()
                .unmarshal(this.getClass().getResource(PROCESS1_XML));
        STORE.publish(EntityType.PROCESS, process1);
        Process process2 = (Process) EntityType.PROCESS.getUnmarshaller()
                .unmarshal(this.getClass().getResource(PROCESS2_XML));
        STORE.publish(EntityType.PROCESS, process2);
    }

    private void cleanupStore() throws FalconException {
        STORE.remove(EntityType.PROCESS, "late-process1");
        STORE.remove(EntityType.PROCESS, "late-process2");
        STORE.remove(EntityType.FEED, "late-feed1");
        STORE.remove(EntityType.FEED, "late-feed2");
        STORE.remove(EntityType.FEED, "late-feed3");
        STORE.remove(EntityType.CLUSTER, "late-cluster");
    }

    @AfterClass
    public void tearDown() throws Exception {
        cleanupStore();
        dfsCluster.shutdown();
    }
}
