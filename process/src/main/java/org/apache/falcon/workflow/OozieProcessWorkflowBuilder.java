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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.converter.OozieProcessMapper;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.OozieClient;

import java.util.*;

/**
 * Oozie workflow builder for falcon entities.
 */
public class OozieProcessWorkflowBuilder extends OozieWorkflowBuilder<Process> {

    @Override
    public Map<String, Properties> newWorkflowSchedule(Process process, List<String> clusters) throws FalconException {
        Map<String, Properties> propertiesMap = new HashMap<String, Properties>();

        for (String clusterName : clusters) {
            org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, clusterName);
            Properties properties = newWorkflowSchedule(process, processCluster.getValidity().getStart(), clusterName,
                    CurrentUser.getUser());
            if (properties != null) {
                propertiesMap.put(clusterName, properties);
            }
        }
        return propertiesMap;
    }

    private void addOptionalInputProperties(Properties properties, Input in, String clusterName)
        throws FalconException {
        Feed feed = EntityUtil.getEntity(EntityType.FEED, in.getFeed());
        org.apache.falcon.entity.v0.feed.Cluster cluster = FeedHelper.getCluster(feed, clusterName);
        String inName = in.getName();
        properties.put(inName + ".frequency", String.valueOf(feed.getFrequency().getFrequency()));
        properties.put(inName + ".freq_timeunit", mapToCoordTimeUnit(feed.getFrequency().getTimeUnit()).name());
        properties.put(inName + ".timezone", feed.getTimezone().getID());
        properties.put(inName + ".end_of_duration", Timeunit.NONE.name());
        properties.put(inName + ".initial-instance", SchemaHelper.formatDateUTC(cluster.getValidity().getStart()));
        properties.put(inName + ".done-flag", "notused");

        String locPath = FeedHelper.getLocation(feed, LocationType.DATA, clusterName).getPath().replace('$', '%');
        properties.put(inName + ".uri-template",
                new Path(locPath).toUri().getScheme() != null ? locPath : "${nameNode}" + locPath);

        properties.put(inName + ".start-instance", in.getStart());
        properties.put(inName + ".end-instance", in.getEnd());
    }

    private Timeunit mapToCoordTimeUnit(TimeUnit tu) {
        switch (tu) {
        case days:
            return Timeunit.DAY;

        case hours:
            return Timeunit.HOUR;

        case minutes:
            return Timeunit.MINUTE;

        case months:
            return Timeunit.MONTH;

        default:
            throw new IllegalArgumentException("Unhandled time unit " + tu);
        }
    }

    @Override
    public Properties newWorkflowSchedule(Process process, Date startDate, String clusterName, String user)
        throws FalconException {
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, clusterName);
        if (!startDate.before(processCluster.getValidity().getEnd())) {// start time >= end time
            return null;
        }

        Cluster cluster = CONFIG_STORE.get(EntityType.CLUSTER, processCluster.getName());
        Path bundlePath = new Path(ClusterHelper.getLocation(cluster, "staging"), EntityUtil.getStagingPath(process));
        Process processClone = (Process) process.copy();
        EntityUtil.setStartDate(processClone, clusterName, startDate);

        OozieProcessMapper mapper = new OozieProcessMapper(processClone);
        if (!mapper.map(cluster, bundlePath)) {
            return null;
        }

        Properties properties = createAppProperties(clusterName, bundlePath, user);

        //Add libpath
        String libPath = process.getWorkflow().getLib();
        if (!StringUtils.isEmpty(libPath)) {
            String path = libPath.replace("${nameNode}", "");
            properties.put(OozieClient.LIBPATH, "${nameNode}" + path);
        }

        if (process.getInputs() != null) {
            for (Input in : process.getInputs().getInputs()) {
                if (in.isOptional()) {
                    addOptionalInputProperties(properties, in, clusterName);
                }
            }
        }
        return properties;
    }

    @Override
    public Date getNextStartTime(Process process, String cluster, Date now) throws FalconException {
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
        return EntityUtil.getNextStartTime(processCluster.getValidity().getStart(),
                process.getFrequency(), process.getTimezone(), now);
    }

    @Override
    public String[] getWorkflowNames(Process process) {
        return new String[]{EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString()};
    }
}
