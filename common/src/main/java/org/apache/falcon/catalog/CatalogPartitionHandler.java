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
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

/**
 * Listens to JMS message and registers partitions.
 */
public final class CatalogPartitionHandler {
    public static final Logger LOG = LoggerFactory.getLogger(CatalogPartitionHandler.class);

    private static final CatalogPartitionHandler INSTANCE = new CatalogPartitionHandler();
    public static final ConfigurationStore STORE = ConfigurationStore.get();
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    public static final String CATALOG_TABLE = "catalog.table";
    public static final String UPDATE_TIME = "UPDATE_TIME";
    public static final String CREATE_TIME = "CREATE_TIME";

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

    public void handlePartition(String clusterName, String feedName, String pathStr, boolean deleteOperation)
        throws FalconException {
        Feed feed = STORE.get(EntityType.FEED, feedName);
        Cluster cluster = STORE.get(EntityType.CLUSTER, clusterName);
        //Take only the path part without url
        Path path = new Path(new Path(pathStr).toUri().getPath());

        CatalogStorage storage = getStorage(feed, cluster);
        if (storage == null) {
            //no table defined for the feed
            return;
        }

        LOG.info("Handle partition for feed {} for path {} - drop?{}", feedName, pathStr, deleteOperation);
        //get date from fs path
        Date date = getDate(feed, clusterName, path.toString());

        Map<String, String> staticPartitions = storage.getPartitions();
        ExpressionHelper.setReferenceDate(date, UTC);
        for (Entry<String, String> entry : staticPartitions.entrySet()) {
            staticPartitions.put(entry.getKey(), evaluator.evaluateFullExpression(entry.getValue(), String.class));
        }

        if (deleteOperation) {
            dropPartition(storage, staticPartitions.values());
            return;

        } else {
            //generate operation
            FileSystem fs =
                HadoopClientFactory.get().createFileSystem(path.toUri(), ClusterHelper.getConfiguration(cluster));
            try {
                if (!fs.exists(path)) {
                    LOG.info("No-op for feed {} as {} doesn't exist", feedName, pathStr);
                    dropPartition(storage, staticPartitions.values());
                    return;
                }
            } catch (IOException e) {
                throw new FalconException(e);
            }

            Map<List<String>, String> finalPartitions = new HashMap<List<String>, String>();
            if (feed.getPartitions() != null) {
                //dynamic partitions
                List<Partition> feedParts = feed.getPartitions().getPartitions();
                try {
                    FileStatus[] files = fs.globStatus(new Path(path, repeat("*", "/", feedParts.size())));
                    if (files == null) {
                        throw new FalconException("Output path " + path + " doesn't exist!");
                    }
                    if (files.length == 0) {
                        throw new FalconException("Partition mismatch for feed " + feedName + " for data path " + path);
                    }
                    for (FileStatus file : files) {
                        Map<String, String> partitions = new LinkedHashMap<String, String>();
                        partitions.putAll(staticPartitions);
                        String[] dynParts =
                            StringUtils.stripStart(file.getPath().toUri().getPath(), path.toString()).split("/");
                        if (!file.isDir() || dynParts.length != feedParts.size()) {
                            throw new FalconException("Partition mismatch for feed " + feedName + " for data path "
                                + file.getPath());
                        }

                        for (int index = 0; index < dynParts.length; index++) {
                            partitions.put(feedParts.get(index).getName(), dynParts[index]);
                        }
                        finalPartitions.put(new ArrayList<String>(partitions.values()), file.getPath().toString());
                    }
                } catch (IOException e) {
                    throw new FalconException(e);
                }
            } else {
                //only static partitions
                finalPartitions.put(new ArrayList<String>(staticPartitions.values()), pathStr);
            }
            registerPartitions(storage, new ArrayList<String>(staticPartitions.values()), finalPartitions);
        }
    }

    private String repeat(String repeatStr, String delim, int cnt) {
        String[] array = new String[cnt];
        for (int index = 0; index < cnt; index++) {
            array[index] = repeatStr;
        }
        return StringUtils.join(array, delim);
    }

    private void dropPartition(CatalogStorage storage, Collection<String> values) throws FalconException {
        HiveMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
        try {
            LOG.info("Dropping partition {} for table {}.{}", values, storage.getDatabase(), storage.getTable());
            client.dropPartition(storage.getDatabase(), storage.getTable(), new ArrayList<String>(values), false);
        } catch (NoSuchObjectException ignore) {
            //ignore
        } catch (TException e) {
            throw new FalconException(e);
        }
    }

    private void registerPartitions(CatalogStorage storage, List<String> staticPartition,
        Map<List<String>, String> finalPartsMap) throws FalconException {
        try {
            HiveMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
            List<org.apache.hadoop.hive.metastore.api.Partition> existParts =
                client.listPartitions(storage.getDatabase(), storage.getTable(), staticPartition, (short) -1);
            List<List<String>> existPartValues = new ArrayList<List<String>>();

            Collection<List<String>> finalPartsValues = finalPartsMap.keySet();
            Table table = null;
            for (org.apache.hadoop.hive.metastore.api.Partition part : existParts) {
                if (finalPartsValues.contains(part.getValues())) {
                    //update partition
                    String location = finalPartsMap.get(part.getValues());
                    updatePartition(storage, part, location);
                } else {
                    //drop partition
                    dropPartition(storage, part.getValues());
                }
                existPartValues.add(part.getValues());
            }

            for (Entry<List<String>, String> entry : finalPartsMap.entrySet()) {
                if (!existPartValues.contains(entry.getKey())) {
                    //Add partition
                    table = addPartition(storage, table, entry.getKey(), entry.getValue());
                }
            }

        } catch (MetaException e) {
            throw new FalconException(e);
        } catch (NoSuchObjectException e) {
            throw new FalconException(e);
        } catch (TException e) {
            throw new FalconException(e);
        }
    }

    private void updatePartition(CatalogStorage storage, org.apache.hadoop.hive.metastore.api.Partition part,
        String location) throws FalconException {
        part.getSd().setLocation(location);
        part.setLastAccessTime((int) (System.currentTimeMillis() / 1000));
        if (part.getParameters() == null) {
            part.setParameters(new HashMap<String, String>());
        }
        part.getParameters().put(UPDATE_TIME, String.valueOf(System.currentTimeMillis()));
        LOG.info("Updating partition {} for {}.{} with location {}", part.getValues(), storage.getDatabase(),
            storage.getTable(), location);
        HiveMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
        try {
            client.alter_partition(storage.getDatabase(), storage.getTable(), part);
        } catch (TException e) {
            throw new FalconException(e);
        }
    }

    private Table addPartition(CatalogStorage storage, Table table, List<String> values,
        String location) throws FalconException {
        HiveMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
        try {
            if (table == null) {
                table = client.getTable(storage.getDatabase(), storage.getTable());
            }
            org.apache.hadoop.hive.metastore.api.Partition part = new org.apache.hadoop.hive.metastore.api.Partition();
            part.setDbName(storage.getDatabase());
            part.setTableName(storage.getTable());
            part.setValues(values);
            part.setSd(table.getSd());
            part.getSd().setLocation(location);
            part.setParameters(table.getParameters());
            if (part.getParameters() == null) {
                part.setParameters(new HashMap<String, String>());
            }
            part.getParameters().put(CREATE_TIME, String.valueOf(System.currentTimeMillis()));
            LOG.info("Adding partition {} to {}.{} with location {}", values, storage.getDatabase(), storage.getTable(),
                location);
            client.add_partition(part);
        } catch (TException e) {
            throw new FalconException(e);
        }
        return table;
    }

    private HiveMetaStoreClient getMetastoreClient(String catalogUrl) throws FalconException {
        HiveConf hiveConf = HiveCatalogService.createHiveConf(catalogUrl);
        try {
            return HCatUtil.getHiveClient(hiveConf);
        } catch (MetaException e) {
            throw new FalconException(e);
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }

    private CatalogStorage getStorage(Feed feed, Cluster cluster) throws FalconException {
        String tableUri = getTableUri(feed);
        if (tableUri == null) {
            return null;
        }

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
                    return prop.getValue().replace("{", "${");
                }
            }
        }

        return null;
    }
}
