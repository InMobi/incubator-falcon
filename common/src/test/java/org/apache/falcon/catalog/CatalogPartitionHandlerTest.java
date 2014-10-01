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

package org.apache.falcon.catalog;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Properties;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.util.HiveTestUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.common.HCatException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Tests plugin that registers partition on succeeded message.
 */
public class CatalogPartitionHandlerTest extends AbstractTestBase {
    private static final int METASTORE_PORT = 49083;
    private static final String CATALOG_DB = "default";
    public static final Path DATA_PATH = new Path("/projects/falcon/clicks/2014/06/18/18");
    public static final String CATALOG_TABLE = "clicks";

    private Thread hcatServer;
    private EmbeddedCluster embeddedCluster;
    private String metastoreUrl;
    private CatalogPartitionHandler partHandler;

    @BeforeClass
    public void setup() throws Exception {
        embeddedCluster = EmbeddedCluster.newCluster("testCluster");
        final String fsUrl = ClusterHelper.getStorageUrl(embeddedCluster.getCluster());

        hcatServer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    HiveConf hiveconf = new HiveConf();
                    hiveconf.set("hive.metastore.warehouse.dir", new File("target/metastore").getAbsolutePath());
                    hiveconf.set("fs.default.name", fsUrl);
                    HiveMetaStore.startMetaStore(METASTORE_PORT, null, hiveconf);
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }
        });
        hcatServer.start();
        metastoreUrl = ClusterHelper.getRegistryEndPoint(embeddedCluster.getCluster());
        HiveTestUtils.createDatabase(metastoreUrl, "default");
        partHandler = CatalogPartitionHandler.get();
    }

    @BeforeMethod
    public void prepare() throws Exception {
        cleanupStore();
        HiveTestUtils.dropTable(metastoreUrl, CATALOG_DB, CATALOG_TABLE);
        FileSystem fs = embeddedCluster.getFileSystem();
        fs.delete(DATA_PATH, true);

        Cluster cluster = embeddedCluster.getCluster();
        store.publish(EntityType.CLUSTER, cluster);
    }

    @AfterClass
    public void cleanup() {
        hcatServer.stop();
    }

    private Feed createFeed(boolean clearPartitions) throws Exception {
        Feed feed = (Feed) storeEntity(EntityType.FEED, "feed" + RandomStringUtils.randomAlphanumeric(10));
        if (clearPartitions) {
            feed.setPartitions(null);
            HiveTestUtils.createExternalTable(metastoreUrl, CATALOG_DB, CATALOG_TABLE, Arrays.asList("ds"),
                "/projects/falcon/clicks");
        } else {
            HiveTestUtils.createExternalTable(metastoreUrl, CATALOG_DB, CATALOG_TABLE, Arrays.asList("ds", "country",
                    "region"), "/projects/falcon/clicks");
        }
        feed.setProperties(getProperties(CatalogPartitionHandler.CATALOG_TABLE,
            "catalog:default:clicks#ds={YEAR}-{MONTH}-{DAY}-{HOUR}"));
        return feed;
    }

    @Test
    public void testStaticPartitions() throws Exception {
        String clusterName = embeddedCluster.getCluster().getName();
        String feedName = createFeed(true).getName();

        //no partition if data path doesn't exist
        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), false);
        try {
            HiveTestUtils.getPartition(metastoreUrl, CATALOG_DB, CATALOG_TABLE, "ds", "2014-06-18-18");
            Assert.fail("Expected exception!");
        } catch (HCatException e) {
            //expected
        }

        //success case
        FileSystem fs = embeddedCluster.getFileSystem();
        fs.mkdirs(DATA_PATH);
        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), false);
        HCatPartition
            partition = HiveTestUtils.getPartition(metastoreUrl, CATALOG_DB, CATALOG_TABLE, "ds", "2014-06-18-18");
        Assert.assertNotNull(partition);
        Assert.assertEquals(new Path(partition.getLocation()).toUri().getPath(), DATA_PATH.toString());

        //re-run scenario
        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), false);
        HCatPartition
            newPartition = HiveTestUtils.getPartition(metastoreUrl, CATALOG_DB, CATALOG_TABLE, "ds", "2014-06-18-18");
        Assert.assertNotNull(newPartition);
        //validate that its the same old partition updated
        Assert.assertEquals(newPartition.getCreateTime(), partition.getCreateTime());
        Assert.assertEquals(new Path(newPartition.getLocation()).toUri().getPath(), DATA_PATH.toString());
    }

    @Test
    public void testEviction() throws Exception {
        String clusterName = embeddedCluster.getCluster().getName();
        String feedName = createFeed(true).getName();
        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), true);

        FileSystem fs = embeddedCluster.getFileSystem();
        fs.mkdirs(DATA_PATH);
        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), false);
        HCatPartition
            partition = HiveTestUtils.getPartition(metastoreUrl, CATALOG_DB, CATALOG_TABLE, "ds", "2014-06-18-18");
        Assert.assertNotNull(partition);

        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), true);
        try {
            HiveTestUtils.getPartition(metastoreUrl, CATALOG_DB, CATALOG_TABLE, "ds", "2014-06-18-18");
            Assert.fail("expected HCatException");
        } catch (HCatException expected) {
            //expected
        }
    }

    @Test
    public void testDynamicPartitions() throws Exception {
        String clusterName = embeddedCluster.getCluster().getName();
        String feedName = createFeed(false).getName();
        FileSystem fs = embeddedCluster.getFileSystem();

        //no dynamic parts, partition should be registered with *
        fs.mkdirs(DATA_PATH);
        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), false);
        List<HCatPartition> partitions =
                HiveTestUtils.getPartitions(metastoreUrl, CATALOG_DB, CATALOG_TABLE, "ds", "2014-06-18-18");
        Assert.assertNotNull(partitions);
        Assert.assertEquals(partitions.size(), 1);
        HCatPartition part = partitions.get(0);
        Assert.assertTrue(part.getValues().equals(Arrays.asList("2014-06-18-18", "*", "*")));

        //success case
        fs.mkdirs(new Path(DATA_PATH, "US/CA"));
        fs.mkdirs(new Path(DATA_PATH, "IND/KA"));
        //Temporary files should not be considered
        fs.mkdirs(new Path(DATA_PATH, "IND/.log"));
        fs.create(new Path(DATA_PATH, "IND/_SUCCESS")).close();
        fs.create(new Path(DATA_PATH, "IND/part-file")).close();
        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), false);
        partitions = HiveTestUtils.getPartitions(metastoreUrl, CATALOG_DB, CATALOG_TABLE, "ds", "2014-06-18-18");
        Assert.assertNotNull(partitions);
        Assert.assertEquals(partitions.size(), 2);

        HCatPartition oldPart = partitions.get(0);
        Assert.assertTrue(oldPart.getValues().equals(Arrays.asList("2014-06-18-18", "IND", "KA")));
        Assert.assertEquals(new Path(oldPart.getLocation()).toUri().getPath(),
            new Path(DATA_PATH, "IND/KA").toString());

        Assert.assertTrue(partitions.get(1).getValues().equals(Arrays.asList("2014-06-18-18", "US", "CA")));
        Assert.assertEquals(new Path(partitions.get(1).getLocation()).toUri().getPath(),
            new Path(DATA_PATH, "US/CA").toString());

        //re-run scenario
        fs.delete(DATA_PATH, true);
        fs.mkdirs(new Path(DATA_PATH, "IND/TN"));
        fs.mkdirs(new Path(DATA_PATH, "IND/KA"));
        partHandler.handlePartition(clusterName, feedName, DATA_PATH.toString(), false);
        partitions = HiveTestUtils.getPartitions(metastoreUrl, CATALOG_DB, CATALOG_TABLE, "ds", "2014-06-18-18");
        Assert.assertNotNull(partitions);
        Assert.assertEquals(partitions.size(), 2);

        HCatPartition newPart = partitions.get(0);
        Assert.assertEquals(newPart.getValues(), Arrays.asList("2014-06-18-18", "IND", "KA"));
        Assert.assertEquals(new Path(newPart.getLocation()).toUri().getPath(), new Path(DATA_PATH,
            "IND/KA").toString());
        Assert.assertEquals(newPart.getParameters().get(CatalogPartitionHandler.CREATE_TIME),
            oldPart.getParameters().get(CatalogPartitionHandler.CREATE_TIME));  //same partition
        Assert.assertTrue(Long.valueOf(newPart.getParameters().get(CatalogPartitionHandler.UPDATE_TIME))
            > Long.valueOf(oldPart.getParameters().get(CatalogPartitionHandler.CREATE_TIME)));

        Assert.assertEquals(partitions.get(1).getValues(), Arrays.asList("2014-06-18-18", "IND", "TN"));
        Assert.assertEquals(new Path(partitions.get(1).getLocation()).toUri().getPath(),
            new Path(DATA_PATH, "IND/TN").toString());
    }

    private Properties getProperties(String name, String value) {
        Properties props = new Properties();
        Property prop = new Property();
        prop.setName(name);
        prop.setValue(value);
        props.getProperties().add(prop);
        return props;
    }
}
