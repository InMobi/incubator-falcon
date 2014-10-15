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
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;

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

    private static final Map<String, IMetaStoreClient> METASTORE_CLIENT_MAP = new HashMap<String, IMetaStoreClient>();

    private static final PathFilter PATH_FILTER = new PathFilter() {
        @Override public boolean accept(Path path) {
            try {
                FileSystem fs = path.getFileSystem(new Configuration());
                return !path.getName().startsWith("_") && !path.getName().startsWith(".") && !fs.isFile(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private ExpressionHelper evaluator = ExpressionHelper.get();

    public static CatalogPartitionHandler get() {
        return INSTANCE;
    }

    public void handlePartition(String clusterName, String feedName, String pathStr, boolean deleteOperation)
        throws FalconException {
        Feed feed = STORE.get(EntityType.FEED, feedName);
        Cluster cluster = STORE.get(EntityType.CLUSTER, clusterName);
        //Take only the path part without url
        Path basePath = new Path(new Path(pathStr).toUri().getPath());

        CatalogStorage storage = getStorage(feed, cluster);
        if (storage == null) {
            //no table defined for the feed
            return;
        }

        LOG.info("Handle partition for feed {} for path {} - drop?{}", feedName, pathStr, deleteOperation);
        //get date from fs path
        Date date = getDate(feed, clusterName, basePath.toString());

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
                HadoopClientFactory.get().createFileSystem(basePath.toUri(), ClusterHelper.getConfiguration(cluster));
            try {
                if (!fs.exists(basePath)) {
                    LOG.info("No-op for feed {} as {} doesn't exist", feedName, pathStr);
                    dropPartition(storage, staticPartitions.values());
                    return;
                }
            } catch (IOException e) {
                throw new FalconException(e);
            }

            Table table = getTable(storage);
            int regPartCnt = table.getPartitionKeys().size();
            int dynPartsCnt = regPartCnt - staticPartitions.size();

            Map<List<String>, String> finalPartitions = new HashMap<List<String>, String>();
            if (dynPartsCnt > 0) {
                try {
                    FileStatus[] files = fs.globStatus(
                            new Path(basePath, org.apache.falcon.util.StringUtils.repeat("*", "/", dynPartsCnt)),
                            PATH_FILTER);

                    if (files != null && files.length > 0) {
                        for (FileStatus file : files) {
                            List<String> partitionValue = new ArrayList<String>(staticPartitions.values());
                            String[] dynParts = getDynamicPartitions(file.getPath(), basePath);
                            for (int index = 0; index < dynParts.length; index++) {
                                partitionValue.add(dynParts[index]);
                            }
                            finalPartitions.put(normalise(partitionValue, regPartCnt), file.getPath().toString());
                        }
                    } else {
                        //no dynamic part files, only static partitions
                        finalPartitions.put(normalise(new ArrayList<String>(staticPartitions.values()), regPartCnt),
                                pathStr);
                    }
                } catch (IOException e) {
                    throw new FalconException(e);
                }
            } else {
                //only static partitions
                finalPartitions.put(normalise(new ArrayList<String>(staticPartitions.values()), regPartCnt), pathStr);
            }
            registerPartitions(storage, table, new ArrayList<String>(staticPartitions.values()), finalPartitions);
        }
    }

    /**
     * make sure that there are correct number of partition values as registered for the table.
     */
    private List<String> normalise(List<String> partitionValue, int regPartCnt) {
        if (partitionValue.size() > regPartCnt) {
            return partitionValue.subList(0, regPartCnt);
        } else if (partitionValue.size() < regPartCnt) {
            //Add 'NODATA' for extra partition values
            partitionValue.addAll(
                    Arrays.asList(StringUtils.repeat("NODATA", "/", regPartCnt - partitionValue.size()).split("/")));
        }
        return partitionValue;
    }

    private Table getTable(CatalogStorage storage) throws FalconException {
        IMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
        try {
            return client.getTable(storage.getDatabase(), storage.getTable());
        } catch (TException e) {
            throw new FalconException(e);
        }
    }

    private String[] getDynamicPartitions(Path path, Path basePath) {
        String dynPart = path.toUri().getPath().substring(basePath.toString().length());
        if (dynPart.startsWith("/")) {      //remove / at start
            dynPart = dynPart.substring(1);
        }
        return dynPart.split("/");
    }

    private void dropPartition(CatalogStorage storage, Collection<String> values) throws FalconException {
        IMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
        try {
            LOG.info("Dropping partition {} for table {}.{}", values, storage.getDatabase(), storage.getTable());
            client.dropPartition(storage.getDatabase(), storage.getTable(), new ArrayList<String>(values), false);
        } catch (NoSuchObjectException ignore) {
            //ignore
        } catch (TException e) {
            throw new FalconException(e);
        }
    }

    private void registerPartitions(CatalogStorage storage, Table table, List<String> staticPartition,
                                    Map<List<String>, String> finalPartsMap) throws FalconException {
        try {
            IMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
            List<org.apache.hadoop.hive.metastore.api.Partition> existParts =
                client.listPartitions(storage.getDatabase(), storage.getTable(), staticPartition, (short) -1);
            List<List<String>> existPartValues = new ArrayList<List<String>>();

            for (org.apache.hadoop.hive.metastore.api.Partition part : existParts) {
                if (finalPartsMap.containsKey(part.getValues())) {
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
                    addPartition(storage, table, entry.getKey(), entry.getValue());
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
        IMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
        try {
            client.alter_partition(storage.getDatabase(), storage.getTable(), part);
        } catch (TException e) {
            throw new FalconException(e);
        }
    }

    private void addPartition(CatalogStorage storage, Table table, List<String> values,
        String location) throws FalconException {
        IMetaStoreClient client = getMetastoreClient(storage.getCatalogUrl());
        try {
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
    }

    private IMetaStoreClient getMetastoreClient(String catalogUrl) throws FalconException {
        if (!METASTORE_CLIENT_MAP.containsKey(catalogUrl)) {
            HiveConf hiveConf = HiveCatalogService.createHiveConf(catalogUrl);
            hiveConf.set(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES.varname,
                    StartupProperties.get().getProperty("catalog.service.retrycount", "3"));
            hiveConf.set(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname,
                    StartupProperties.get().getProperty("catalog.service.retrydelayinsecs", "0"));
            try {
                IMetaStoreClient client = RetryingMetaStoreClient.getProxy(hiveConf, new HiveMetaHookLoader() {
                    @Override
                    public HiveMetaHook getHook(Table table) throws MetaException {
                        return null;
                    }
                }, HiveMetaStoreClient.class.getName());
                METASTORE_CLIENT_MAP.put(catalogUrl, client);
            } catch (MetaException e) {
                throw new FalconException(e);
            }
        }
        return METASTORE_CLIENT_MAP.get(catalogUrl);
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
