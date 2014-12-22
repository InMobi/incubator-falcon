package org.apache.falcon.logging.v2;

import org.apache.falcon.logging.DefaultTaskLogRetriever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hadoop Yarn task log retriever
 */
public class TaskLogRetrieverYarn extends DefaultTaskLogRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(TaskLogRetrieverYarn.class);
    private static final String SCHEME = "http://";
    private static final String YARN_LOG_SERVER_URL = "yarn.log.server.url";

    @Override
    public List<String> retrieveTaskLogURL(String jobIdStr) throws IOException, InterruptedException {
        List<String> taskLogUrls = new ArrayList<String>();
        Configuration conf = getConf();
        Cluster cluster = new Cluster(conf);
        JobID jobID = JobID.forName(jobIdStr);
        if (jobID == null) {
            LOG.warn("External id for workflow action is null");
            return null;
        }
        Job job = cluster.getJob(jobID);
        if (job != null) {
            TaskCompletionEvent[] events = job.getTaskCompletionEvents(0);
            for (TaskCompletionEvent event : events) {
                LogParams params = cluster.getLogParams(jobID, event.getTaskAttemptId());
                String url = SCHEME + conf.get(YARN_LOG_SERVER_URL) + "/"
                        + event.getTaskTrackerHttp() + "/"
                        + params.getContainerId() + "/"
                        + params.getApplicationId() + "/"
                        + params.getOwner() + "?start=0";
                LOG.info("Task Log URL for the job {} is {}" + jobIdStr, url);
                taskLogUrls.add(url);
            }
            return taskLogUrls;
        }
        LOG.warn("Unable to find the job in cluster {}" + jobIdStr);
        return null;
    }

}
