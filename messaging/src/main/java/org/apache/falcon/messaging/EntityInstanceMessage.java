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

package org.apache.falcon.messaging;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Value Object which is stored in JMS Topic as MapMessage.
 */
public class EntityInstanceMessage {

    private final Map<ARG, String> keyValueMap = new LinkedHashMap<ARG, String>();
    private static final Logger LOG = LoggerFactory.getLogger(EntityInstanceMessage.class);
    private static final String FALCON_ENTITY_TOPIC_NAME = "FALCON.ENTITY.TOPIC";

    /**
     * Feed Entity operations supported.
     */
    public enum EntityOps {
        GENERATE, DELETE, ARCHIVE, REPLICATE, CHMOD
    }

    /**
     * properties available in feed entity operation workflow.
     */
    public enum ARG {
        entityName("entityName"),
        feedNames("feedNames"),
        feedInstancePaths("feedInstancePaths"),
        workflowId("workflowId"),
        runId("runId"),
        nominalTime("nominalTime"),
        timeStamp("timeStamp"),
        brokerUrl("broker.url"),
        brokerImplClass("broker.impl.class"),
        entityType("entityType"),
        operation("operation"),
        logFile("logFile"),
        topicName("topicName"),
        status("status"),
        brokerTTL("broker.ttlInMins"),
        cluster("cluster"),
        workflowUser("workflowUser"),
        logDir("logDir");

        private String propName;

        private ARG(String propName) {
            this.propName = propName;
        }

        /**
         * @return Name of the Argument used in the parent workflow to pass
         *         arguments to MessageProducer Main class.
         */
        public String getArgName() {
            return this.name();
        }

        /**
         * @return Name of the property used in the startup.properties,
         *         coordinator and parent workflow.
         */
        public String getPropName() {
            return this.propName;
        }
    }

    public Map<ARG, String> getKeyValueMap() {
        return this.keyValueMap;
    }

    public String getTopicName() {
        return this.keyValueMap.get(ARG.topicName);
    }

    public String getFeedName() {
        return this.keyValueMap.get(ARG.feedNames);
    }

    public void setFeedName(String feedName) {
        this.keyValueMap.remove(ARG.feedNames);
        this.keyValueMap.put(ARG.feedNames, feedName);
    }

    public String getFeedInstancePath() {
        return this.keyValueMap.get(ARG.feedInstancePaths);
    }

    public void setFeedInstancePath(String feedInstancePath) {
        this.keyValueMap.remove(ARG.feedInstancePaths);
        this.keyValueMap.put(ARG.feedInstancePaths, feedInstancePath);
    }

    public String getEntityType() {
        return this.keyValueMap.get(ARG.entityType);
    }

    public String getBrokerTTL() {
        return this.keyValueMap.get(ARG.brokerTTL);
    }

    public void convertDateFormat() throws FalconException {
        try {
            String date = this.keyValueMap.remove(ARG.nominalTime);
            this.keyValueMap.put(ARG.nominalTime, getFalconDate(date));
            date = this.keyValueMap.remove(ARG.timeStamp);
            this.keyValueMap.put(ARG.timeStamp, getFalconDate(date));
        } catch(ParseException e) {
            throw new FalconException(e);
        }
    }

    public static List<EntityInstanceMessage> getMessages(CommandLine cmd) throws ParseException, FalconException {
        String topicName = cmd.getOptionValue(ARG.topicName.getArgName());
        EntityType entityType = EntityType.valueOf(cmd.getOptionValue(ARG.entityType.getArgName()).toUpperCase());

        List<EntityInstanceMessage> messages = new ArrayList<EntityInstanceMessage>();

        if (topicName.equals(FALCON_ENTITY_TOPIC_NAME) || entityType == EntityType.PROCESS) {
            //One message if its system topic || user topic for process
            EntityOps operation = EntityOps.valueOf(cmd.getOptionValue(ARG.operation.getArgName()));
            String feedNames = cmd.getOptionValue(ARG.feedNames.getArgName());
            String feedPaths = cmd.getOptionValue(ARG.feedInstancePaths.getArgName());
            if (operation == EntityOps.DELETE) {
                Path logFile = new Path(cmd.getOptionValue(ARG.logFile.getArgName()));
                feedPaths = getInstancePathsFromFile(logFile);
                if (feedPaths != null) {
                    feedNames = repeat(feedNames, ",", feedPaths.split(",").length);
                }
            }
            messages.add(createMessage(cmd, feedNames, feedPaths));
        } else {
            //one message per feed name
            Map<String, String> feedMap = getFeedPaths(cmd);
            for (Entry<String, String> entry : feedMap.entrySet()) {
                messages.add(createMessage(cmd, entry.getKey(), entry.getValue()));
            }
        }
        return messages;
    }

    //Could have used StringUtils.repeat, but is not in commons-lang-2.4
    private static String repeat(String pattern, String delim, int cnt) {
        String[] list = new String[cnt];
        for(int index = 0; index < cnt; index++) {
            list[index] = pattern;
        }
        return StringUtils.join(list, delim);
    }

    private static EntityInstanceMessage createMessage(CommandLine cmd, String feedNames, String feedPaths)
        throws FalconException {
        EntityInstanceMessage message = new EntityInstanceMessage();
        for (ARG arg : ARG.values()) {
            message.keyValueMap.put(arg, cmd.getOptionValue(arg.name()));
        }
        message.convertDateFormat();
        message.setFeedName(feedNames);
        message.setFeedInstancePath(feedPaths);
        return message;
    }

    private static String getInstancePathsFromFile(Path path) throws FalconException {
        InputStream instance = null;
        try {
            FileSystem fs = path.getFileSystem(new Configuration());

            if (fs.exists(path)) {
                ByteArrayOutputStream writer = new ByteArrayOutputStream();
                instance = fs.open(path);
                IOUtils.copyBytes(instance, writer, 4096, true);
                String[] instancePaths = writer.toString().split("=");
                if (instancePaths.length > 1) {
                    return instancePaths[1];
                }
            }
        } catch(IOException e) {
            throw new FalconException(e);
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(instance);
        }
        return null;
    }

    //returns map of feed name and feed paths
    private static Map<String, String> getFeedPaths(CommandLine cmd) throws FalconException {
        EntityOps operation = EntityOps.valueOf(cmd.getOptionValue(ARG.operation.getArgName()));
        Map<String, String> feedMap = new HashMap<String, String>();
        String[] feedNames = cmd.getOptionValue(ARG.feedNames.getArgName()).split(",");
        if (operation == EntityOps.DELETE) {
            Path logFile = new Path(cmd.getOptionValue(ARG.logFile.getArgName()));
            feedMap.put(feedNames[0], getInstancePathsFromFile(logFile));
        } else {
            String[] feedPaths = cmd.getOptionValue(ARG.feedInstancePaths.getArgName()).split(",");
            assert feedNames.length == feedPaths.length;
            for (int index = 0; index < feedNames.length; index++) {
                String feedPath = feedMap.get(feedNames[index]);
                feedPath = (feedPath == null ? feedPaths[index] : (feedPath + "," + feedPaths[index]));
                feedMap.put(feedNames[index], feedPath);
            }
        }
        return feedMap;
    }

    public String getFalconDate(String nominalTime) throws ParseException {
        DateFormat nominalFormat = new SimpleDateFormat(
                "yyyy'-'MM'-'dd'-'HH'-'mm");
        Date nominalDate = nominalFormat.parse(nominalTime);
        DateFormat falconFormat = new SimpleDateFormat(
                "yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        return falconFormat.format(nominalDate);
    }
}
