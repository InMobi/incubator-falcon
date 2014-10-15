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

package org.apache.falcon.util;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.logging.LogMover;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.workflow.engine.OozieClientFactory;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.ProxyOozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Oozie Utility class for integration-tests.
 */
public final class OozieTestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OozieTestUtils.class);

    private OozieTestUtils() {
    }

    public static ProxyOozieClient getOozieClient(TestContext context) throws FalconException {
        return getOozieClient(context.getCluster().getCluster());
    }

    public static ProxyOozieClient getOozieClient(Cluster cluster) throws FalconException {
        return OozieClientFactory.get(cluster);
    }

    public static List<BundleJob> getBundles(TestContext context) throws Exception {
        List<BundleJob> bundles = new ArrayList<BundleJob>();
        if (context.getClusterName() == null) {
            return bundles;
        }

        ProxyOozieClient ozClient = OozieClientFactory.get(context.getCluster().getCluster());
        return ozClient.getBundleJobsInfo("name=FALCON_PROCESS_" + context.getProcessName(), 0, 10);
    }

    public static boolean killOozieJobs(TestContext context) throws Exception {
        if (context.getCluster() == null) {
            return true;
        }

        ProxyOozieClient ozClient = getOozieClient(context);
        List<BundleJob> bundles = getBundles(context);
        if (bundles != null) {
            for (BundleJob bundle : bundles) {
                ozClient.kill(bundle.getId());
            }
        }

        return false;
    }

    public static void waitForProcessWFtoStart(TestContext context) throws Exception {
        waitForWorkflowStart(context, context.getProcessName());
    }

    public static void waitForWorkflowStart(TestContext context, String entityName) throws Exception {
        for (int i = 0; i < 10; i++) {
            List<WorkflowJob> jobs = getRunningJobs(context, entityName);
            if (jobs != null && !jobs.isEmpty()) {
                return;
            }

            System.out.println("Waiting for workflow to start");
            Thread.sleep(i * 1000);
        }

        throw new Exception("Workflow for " + entityName + " hasn't started in oozie");
    }

    private static List<WorkflowJob> getRunningJobs(TestContext context, String entityName) throws Exception {
        ProxyOozieClient ozClient = getOozieClient(context);
        return ozClient.getJobsInfo(
                ProxyOozieClient.FILTER_STATUS + '=' + Job.Status.RUNNING + ';'
                        + ProxyOozieClient.FILTER_NAME + '=' + "FALCON_PROCESS_DEFAULT_" + entityName);
    }

    public static void waitForBundleStart(TestContext context, Job.Status... status) throws Exception {
        ProxyOozieClient ozClient = getOozieClient(context);
        List<BundleJob> bundles = getBundles(context);
        if (bundles.isEmpty()) {
            return;
        }

        Set<Job.Status> statuses = new HashSet<Job.Status>(Arrays.asList(status));
        Job.Status lastStatus = null;
        String bundleId = bundles.get(0).getId();
        for (int i = 0; i < 15; i++) {
            Thread.sleep(i * 1000);
            BundleJob bundle = ozClient.getBundleJobInfo(bundleId);
            lastStatus = bundle.getStatus();
            if (statuses.contains(bundle.getStatus())) {
                if (statuses.contains(Job.Status.FAILED) || statuses.contains(Job.Status.KILLED)) {
                    return;
                }

                boolean done = false;
                for (CoordinatorJob coord : bundle.getCoordinators()) {
                    if (statuses.contains(coord.getStatus())) {
                        done = true;
                    }
                }
                if (done) {
                    return;
                }
            }
            LOG.info("Waiting for bundle {} in {} state. Current status {}", bundleId, statuses, bundle.getStatus());
        }
        throw new Exception("Bundle " + bundleId + " is not " + statuses + " in oozie. last status " + lastStatus);
    }

    public static WorkflowJob getWorkflowJob(Cluster cluster, String filter) throws Exception {
        ProxyOozieClient ozClient = getOozieClient(cluster);

        List<WorkflowJob> jobs;
        while (true) {
            jobs = ozClient.getJobsInfo(filter);
            System.out.println("jobs = " + jobs);
            if (jobs.size() > 0) {
                break;
            } else {
                Thread.sleep(1000);
            }
        }

        WorkflowJob jobInfo = jobs.get(0);
        while (true) {
            if (!(jobInfo.getStatus() == WorkflowJob.Status.RUNNING
                    || jobInfo.getStatus() == WorkflowJob.Status.PREP)) {
                break;
            } else {
                Thread.sleep(1000);
                jobInfo = ozClient.getJobInfo(jobInfo.getId());
                System.out.println("jobInfo = " + jobInfo);
            }
        }

        return jobInfo;
    }

    public static Path getOozieLogPath(Cluster cluster, WorkflowJob jobInfo) throws Exception {
        Path stagingPath = EntityUtil.getLogPath(cluster, cluster);
        final Path logPath = new Path(ClusterHelper.getStorageUrl(cluster), stagingPath);
        LogMover.main(new String[] {
            "-workflowEngineUrl", ClusterHelper.getOozieUrl(cluster),
            "-subflowId", jobInfo.getId(), "-runId", "1",
            "-logDir", logPath.toString() + "/job-2012-04-21-00-00",
            "-status", "SUCCEEDED", "-entityType", "process",
            "-userWorkflowEngine", "pig",
        });

        return new Path(logPath, "job-2012-04-21-00-00/001/oozie.log");
    }
}
