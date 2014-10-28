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
package org.apache.falcon.rerun.handler;

import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.process.LateInput;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.latedata.LateDataHandler;
import org.apache.falcon.rerun.event.LaterunEvent;
import org.apache.falcon.rerun.queue.DelayedQueue;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.*;

/**
 * A consumer of late reruns.
 *
 * @param <T>
 */
public class LateRerunConsumer<T extends LateRerunHandler<DelayedQueue<LaterunEvent>>>
        extends AbstractRerunConsumer<LaterunEvent, T> {

    public LateRerunConsumer(T handler) {
        super(handler);
    }

    @Override
    protected void handleRerun(String clusterName, String jobStatus,
                               LaterunEvent message) {
        try {
            if (jobStatus.equals("RUNNING") || jobStatus.equals("PREP")
                    || jobStatus.equals("SUSPENDED")) {
                LOG.debug("Re-enqueing message in LateRerunHandler for workflow with same delay as "
                    + "job status is running: {}", message.getWfId());
                message.setMsgInsertTime(System.currentTimeMillis());
                handler.offerToQueue(message);
                return;
            }
            Properties properties =
                    handler.getWfEngine().getWorkflowProperties(message.getClusterName(), message.getWfId());
            Entity entity = EntityUtil.getEntity(message.getEntityType(), message.getEntityName());
            String loggedNominalTime = properties.getProperty("nominalTime");
            String nominalTime = SchemaHelper.formatDateUTCToISO8601(loggedNominalTime, "yyyy-MM-dd-HH-mm");
            if (handler.getEventDelay(entity, nominalTime) == -1) {
                LOG.warn("Late rerun expired for entity: {} after de-queue", entity);
                return;
            }

            String detectLate = detectLate(message, properties);

            if (detectLate.equals("")) {
                LOG.debug("No Late Data Detected, scheduling next late rerun for wf-id: {} at {}",
                        message.getWfId(), SchemaHelper.formatDateUTC(new Date()));
                handler.handleRerun(clusterName, message.getEntityType(), message.getEntityName(),
                        message.getInstance(), Integer.toString(message.getRunId()),
                        message.getWfId(), message.getWorkflowUser(), System.currentTimeMillis());
                return;
            }

            LOG.info("Late changes detected in the following feeds: {}", detectLate);

            handler.getWfEngine().reRun(message.getClusterName(), message.getWfId(), null);
            LOG.info("Scheduled late rerun for wf-id: {} on cluster: {}",
                    message.getWfId(), message.getClusterName());
        } catch (Exception e) {
            LOG.warn("Late Re-run failed for instance {}:{} after {}",
                    message.getEntityName(), message.getInstance(), message.getDelayInMilliSec(), e);
            GenericAlert.alertLateRerunFailed(message.getEntityType(), message.getEntityName(),
                    message.getInstance(), message.getWfId(), message.getWorkflowUser(),
                    Integer.toString(message.getRunId()), e.getMessage());
        }
    }

    public String detectLate(LaterunEvent message, Properties properties) throws Exception {
        LateDataHandler late = new LateDataHandler();
        String falconInputFeeds = properties.getProperty("falconInputNames");
        String falconInPaths = properties.getProperty("falconInPaths");
        String falconInputFeedStorageTypes = properties.getProperty("falconInputFeedStorageTypes");
        String logDir = properties.getProperty("logDir");
        String nominalTime = properties.getProperty("nominalTime");
        String srcClusterName = properties.getProperty("srcClusterName");
        Path lateLogPath = handler.getLateLogPath(logDir, nominalTime, srcClusterName);

        final String storageEndpoint = properties.getProperty(AbstractWorkflowEngine.NAME_NODE);
        Configuration conf = LateRerunHandler.getConfiguration(storageEndpoint);
        FileSystem fs = HadoopClientFactory.get().createFileSystem(conf);
        if (!fs.exists(lateLogPath)) {
            LOG.warn("Late log file: {} not found", lateLogPath);
            return "";
        }

        String[] pathGroups = falconInPaths.split("#");
        String[] inputFeeds = falconInputFeeds.split("#");
        String[] inputFeedStorageTypes = falconInputFeedStorageTypes.split("#");

        Map<String, Long> computedMetrics = new LinkedHashMap<String, Long>();
        Entity entity = EntityUtil.getEntity(message.getEntityType(), message.getEntityName());
        if (EntityUtil.getLateProcess(entity) != null) {
            List<String> lateFeed = new ArrayList<String>();
            for (LateInput li : EntityUtil.getLateProcess(entity).getLateInputs()) {
                lateFeed.add(li.getInput());
            }

            for (int index = 0; index < pathGroups.length; index++) {
                if (lateFeed.contains(inputFeeds[index])) {
                    long computedMetric = late.computeStorageMetric(
                            pathGroups[index], inputFeedStorageTypes[index], conf);
                    computedMetrics.put(inputFeeds[index], computedMetric);
                }
            }
        } else {
            LOG.warn("Late process is not configured for entity: {} ({})",
                    message.getEntityType(), message.getEntityName());
        }

        return late.detectChanges(lateLogPath, computedMetrics, conf);
    }

}
