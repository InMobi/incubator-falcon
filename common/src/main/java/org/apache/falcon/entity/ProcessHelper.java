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

package org.apache.falcon.entity;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Helper methods for accessing process members.
 */
public final class ProcessHelper {

    private ProcessHelper() {}

    public static Cluster getCluster(Process process, String clusterName) {
        for (Cluster cluster : process.getClusters().getClusters()) {
            if (cluster.getName().equals(clusterName)) {
                return cluster;
            }
        }
        return null;
    }

    public static String getProcessWorkflowName(String workflowName, String processName) {
        return StringUtils.isEmpty(workflowName) ? processName + "-workflow" : workflowName;
    }

    public static Storage.TYPE getStorageType(org.apache.falcon.entity.v0.cluster.Cluster cluster,
                                              Process process) throws FalconException {
        Storage.TYPE storageType = Storage.TYPE.FILESYSTEM;
        if (process.getInputs() == null && process.getOutputs() == null) {
            return storageType;
        }

        if (process.getInputs() != null) {
            for (Input input : process.getInputs().getInputs()) {
                Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
                storageType = FeedHelper.getStorageType(feed, cluster);
                if (Storage.TYPE.TABLE == storageType) {
                    break;
                }
            }
        }

        // If input feeds storage type is file system check storage type of output feeds
        if (process.getOutputs() != null && Storage.TYPE.FILESYSTEM == storageType) {
            for (Output output : process.getOutputs().getOutputs()) {
                Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
                storageType = FeedHelper.getStorageType(feed, cluster);
                if (Storage.TYPE.TABLE == storageType) {
                    break;
                }
            }
        }

        return storageType;
    }

    public static Path getUserWorkflowPath(Process process, org.apache.falcon.entity.v0.cluster.Cluster cluster,
        Path buildPath) throws FalconException {
        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(ClusterHelper.getConfiguration(cluster));
            Path wfPath = new Path(process.getWorkflow().getPath());
            if (fs.isFile(wfPath)) {
                return new Path(buildPath.getParent(), EntityUtil.PROCESS_USER_DIR + "/" + wfPath.getName());
            } else {
                return new Path(buildPath.getParent(), EntityUtil.PROCESS_USER_DIR);
            }
        } catch(IOException e) {
            throw new FalconException("Failed to get workflow path", e);
        }
    }

    public static Path getUserLibPath(Process process, org.apache.falcon.entity.v0.cluster.Cluster cluster,
        Path buildPath) throws FalconException {
        try {
            if (process.getWorkflow().getLib() == null) {
                return null;
            }
            Path libPath = new Path(process.getWorkflow().getLib());

            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(ClusterHelper.getConfiguration(cluster));
            if (fs.isFile(libPath)) {
                return new Path(buildPath, EntityUtil.PROCESS_USERLIB_DIR + "/" + libPath.getName());
            } else {
                return new Path(buildPath, EntityUtil.PROCESS_USERLIB_DIR);
            }
        } catch(IOException e) {
            throw new FalconException("Failed to get user lib path", e);
        }
    }
}
