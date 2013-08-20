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

package org.apache.falcon.workflow.engine;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.InstancesResult;

import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Workflow engine should minimally support the
 * following operations.
 */
public abstract class AbstractWorkflowEngine {

    public static final String NAME_NODE = "nameNode";
    public static final String JOB_TRACKER = "jobTracker";

    protected Set<WorkflowEngineActionListener> listeners = new HashSet<WorkflowEngineActionListener>();

    public void registerListener(WorkflowEngineActionListener listener) {
        listeners.add(listener);
    }

    public abstract boolean isAlive(Cluster cluster) throws FalconException;

    public abstract void schedule(Entity entity) throws FalconException;

    public abstract String suspend(Entity entity) throws FalconException;

    public abstract String resume(Entity entity) throws FalconException;

    public abstract String delete(Entity entity) throws FalconException;

    public abstract String delete(Entity entity, String cluster) throws FalconException;

    public abstract void reRun(String cluster, String wfId, Properties props) throws FalconException;

    public abstract boolean isActive(Entity entity) throws FalconException;

    public abstract boolean isSuspended(Entity entity) throws FalconException;

    public abstract InstancesResult getRunningInstances(Entity entity) throws FalconException;

    public abstract InstancesResult killInstances(Entity entity, Date start, Date end, Properties props)
        throws FalconException;

    public abstract InstancesResult reRunInstances(Entity entity, Date start, Date end, Properties props)
        throws FalconException;

    public abstract InstancesResult suspendInstances(Entity entity, Date start, Date end, Properties props)
        throws FalconException;

    public abstract InstancesResult resumeInstances(Entity entity, Date start, Date end, Properties props)
        throws FalconException;

    public abstract InstancesResult getStatus(Entity entity, Date start, Date end) throws FalconException;

    public abstract void update(Entity oldEntity, Entity newEntity, String cluster) throws FalconException;

    public abstract String getWorkflowStatus(String cluster, String jobId) throws FalconException;

    public abstract String getWorkflowProperty(String cluster, String jobId, String property) throws FalconException;

    public abstract InstancesResult getJobDetails(String cluster, String jobId) throws FalconException;
}
