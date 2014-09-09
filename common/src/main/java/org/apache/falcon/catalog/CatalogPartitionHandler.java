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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Partition;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Listens to JMS message and registers partitions.
 */
public final class CatalogPartitionHandler {
    public static final Logger LOG = LoggerFactory.getLogger(CatalogPartitionHandler.class);

    private static final CatalogPartitionHandler INSTANCE = new CatalogPartitionHandler();
    public static final ConfigurationStore STORE = ConfigurationStore.get();
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    public static final String CATALOG_TABLE = "catalog.table";
    private static final ConcurrentHashMap<String, HiveMetaStoreClient> CACHE = new ConcurrentHashMap<String,
        HiveMetaStoreClient>();

    private AbstractCatalogService service;
    private ExpressionHelper evaluator = ExpressionHelper.get();

    private CatalogPartitionHandler() {
        try {
            service = CatalogServiceFactory.getCatalogService();
        } catch (FalconException e) {
            throw new RuntimeException(e);
        }
    }

    public static CatalogPartitionHandler get() {
        return INSTANCE;
    }

    public void registerPartitions(String clusterName, String feedName, String pathStr) throws FalconException {
        Feed feed = STORE.get(EntityType.FEED, feedName);
        Cluster cluster = STORE.get(EntityType.CLUSTER, clusterName);
        Path path = new Path(pathStr);

        //get date from fs path
        Date date = getDate(feed, clusterName, pathStr);

        CatalogStorage storage = getStorage(feed, cluster);

        //static partitions
        Map<String, String> staticPartitions = storage.getPartitions();
        ExpressionHelper.setReferenceDate(date, UTC);
        for (Entry<String, String> entry : staticPartitions.entrySet()) {
            staticPartitions.put(entry.getKey(), evaluator.evaluateFullExpression(entry.getValue(), String.class));
        }

        dropPartitions(storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(), staticPartitions);
        FileSystem fs =
            HadoopClientFactory.get().createFileSystem(path.toUri(), ClusterHelper.getConfiguration(cluster));
        try {
            if (!fs.exists(path)) {
                LOG.info("Not registering partition for " + feedName + " as " + pathStr + " doesn't exist");
                return;
            }
        } catch (IOException e) {
            throw new FalconException(e);
        }

        //dynamic partitions
        if (feed.getPartitions() != null) {
            List<Partition> feedParts = feed.getPartitions().getPartitions();
            try {
                FileStatus[] files = fs.globStatus(new Path(path, StringUtils.repeat("*", "/", feedParts.size())));
                if (files == null) {
                    throw new FalconException("Output path " + path + " doesn't exist!");
                }
                if (files.length == 0) {
                    throw new FalconException("Partition mismatch for feed " + feedName + " for data path " + pathStr);
                }
                for (FileStatus file : files) {
                    Map<String, String> partitions = new HashMap<String, String>();
                    partitions.putAll(staticPartitions);
                    String[] dynParts = StringUtils.stripStart(file.getPath().toUri().getPath(), pathStr).split("/");
                    if (!file.isDir() || dynParts.length != feedParts.size()) {
                        throw new FalconException("Partition mismatch for feed " + feedName + " for data path "
                            + file.getPath());
                    }

                    for (int i = 0; i < dynParts.length; i++) {
                        partitions.put(feedParts.get(i).getName(), dynParts[i]);
                    }
                    service.registerPartition(storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(),
                        partitions, file.getPath().toString());
                }
            } catch (IOException e) {
                throw new FalconException(e);
            }
        } else {
            service.registerPartition(storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(),
                staticPartitions, pathStr);
        }

    }

    private HiveMetaStoreClient getMetastoreClient(String catalogUrl) throws FalconException {
        if (!CACHE.containsKey(catalogUrl)) {
            HiveConf hiveConf = HiveCatalogService.createHiveConf(catalogUrl);
            try {
                CACHE.putIfAbsent(catalogUrl, new HiveMetaStoreClient(hiveConf));
            } catch (MetaException e) {
                throw new FalconException(e);
            }
        }
        return CACHE.get(catalogUrl);
    }

    //Use metastore client as hcat client drops even the path for the partition
    private void dropPartitions(String catalogUrl, String database, String table, Map<String, String> partSpec)
        throws FalconException {
        List<CatalogPartition> partitions = service.listPartitionsByFilter(catalogUrl, database, table, partSpec);
        if (!partitions.isEmpty()) {
            HiveMetaStoreClient client = getMetastoreClient(catalogUrl);
            for (CatalogPartition partition : partitions) {
                try {
                    client.dropPartition(database, table, new ArrayList<String>(partition.getValues()), false);
                } catch (TException e) {
                    throw new FalconException(e);
                }
            }
        }
    }

    private CatalogStorage getStorage(Feed feed, Cluster cluster) throws FalconException {
        String tableUri = getTableUri(feed);
        CatalogTable table = new CatalogTable();
        table.setUri(tableUri);
        try {
            return new CatalogStorage(cluster, table);
        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }
    }

    /**
     * Get date from fs path.
     * @param feed - feed entity for the fs path
     * @param cluster - cluster name
     * @param path - materialised fs path
     * @return date
     */
    private Date getDate(Feed feed, String cluster, String path) {
        String templatePath = null;
        List<Location> locations = FeedHelper.getLocations(FeedHelper.getCluster(feed, cluster), feed);
        for (Location loc : locations) {
            if (loc.getType() == LocationType.DATA) {
                templatePath = loc.getPath();
                break;
            }
        }
        if (templatePath == null) {
            throw new IllegalArgumentException("No data path defined for feed " + feed.getName() + " for cluster "
                + cluster);
        }

        String dateFormat = FeedHelper.getDateFormatInPath(templatePath);
        return FeedHelper.getDate(new Path(path), templatePath, dateFormat, UTC.getDisplayName());
    }

    private String getTableUri(Feed feed) {
        if (feed.getProperties() != null) {
            List<Property> props = feed.getProperties().getProperties();
            for (Property prop : props) {
                if (prop.getName().equals(CATALOG_TABLE)) {
                    return prop.getValue();
                }
            }
        }

        return null;
    }
}
