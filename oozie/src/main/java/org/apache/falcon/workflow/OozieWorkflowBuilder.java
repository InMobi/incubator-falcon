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

package org.apache.falcon.workflow;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.workflow.engine.OozieWorkflowEngine;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Base workflow builder for falcon entities.
 * @param <T>
 */
public abstract class OozieWorkflowBuilder<T extends Entity> extends WorkflowBuilder<T> {

    private static final Logger LOG = Logger.getLogger(OozieWorkflowBuilder.class);
    protected static final ConfigurationStore CONFIG_STORE = ConfigurationStore.get();

    protected Properties createAppProperties(String clusterName, Path bundlePath, String user) throws FalconException {

        Cluster cluster = EntityUtil.getEntity(EntityType.CLUSTER, clusterName);
        Properties properties = new Properties();
        if (cluster.getProperties() != null) {
            addClusterProperties(properties, cluster.getProperties()
                    .getProperties());
        }
        properties.setProperty(OozieWorkflowEngine.NAME_NODE,
                ClusterHelper.getStorageUrl(cluster));
        properties.setProperty(OozieWorkflowEngine.JOB_TRACKER,
                ClusterHelper.getMREndPoint(cluster));
        properties.setProperty(OozieClient.BUNDLE_APP_PATH,
                "${" + OozieWorkflowEngine.NAME_NODE + "}" + bundlePath.toString());
        properties.setProperty("colo.name", cluster.getColo());

        properties.setProperty(OozieClient.USER_NAME, user);
        properties.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");
        properties.setProperty("falcon.libpath", ClusterHelper.getLocation(cluster, "working") + "/lib");
        LOG.info("Cluster: " + cluster.getName() + ", PROPS: " + properties);
        return properties;
    }

    private void addClusterProperties(Properties properties,
                                      List<Property> clusterProperties) {
        for (Property prop : clusterProperties) {
            properties.setProperty(prop.getName(), prop.getValue());
        }
    }

    public abstract Date getNextStartTime(T entity, String cluster, Date now) throws FalconException;
}
