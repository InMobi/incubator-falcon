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
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Message producer used in the workflow to send a message to the queue/topic.
 */
public class MessageProducer extends Configured implements Tool {

    private Connection connection;
    private static final Logger LOG = LoggerFactory.getLogger(MessageProducer.class);
    private static final long DEFAULT_TTL = 3 * 24 * 60 * 60 * 1000;

    /**
     * @param entityInstanceMessage - Accepts a Message to be send to JMS topic, creates a new
     *                  Topic based on topic name if it does not exist or else
     *                  existing topic with the same name is used to send the message.
     * @throws JMSException
     */
    protected void sendMessage(EntityInstanceMessage entityInstanceMessage)
        throws JMSException {

        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        Topic entityTopic = session.createTopic(entityInstanceMessage
                .getTopicName());
        javax.jms.MessageProducer producer = session
                .createProducer(entityTopic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        long messageTTL = DEFAULT_TTL;
        try {
            long messageTTLinMins = Long.parseLong(entityInstanceMessage
                    .getBrokerTTL());
            messageTTL = messageTTLinMins * 60 * 1000;
        } catch (NumberFormatException e) {
            LOG.error("Error in parsing broker.ttl, setting TTL to: {} milli-seconds", DEFAULT_TTL);
        }
        producer.setTimeToLive(messageTTL);
        producer.send(new EntityInstanceMessageCreator(entityInstanceMessage)
                .createMessage(session));
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MessageProducer(), args);
    }

    private void createAndStartConnection(String implementation, String userName,
                                          String password, String url)
        throws JMSException, ClassNotFoundException, InstantiationException,
               IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        Class<ConnectionFactory> clazz = (Class<ConnectionFactory>) MessageProducer.class
                .getClassLoader().loadClass(implementation);

        ConnectionFactory connectionFactory = clazz.getConstructor(
                String.class, String.class, String.class).newInstance(userName,
                password, url);

        connection = connectionFactory.createConnection();
        connection.start();
    }

    private static CommandLine getCommand(String[] arguments)
        throws ParseException {

        Options options = new Options();
        addOption(options, new Option(ARG.brokerImplClass.getArgName(), true,
                "message broker Implementation class"));
        addOption(options, new Option(ARG.brokerTTL.getArgName(), true,
                "message time-to-live"));
        addOption(options, new Option(ARG.brokerUrl.getArgName(), true,
                "message broker url"));
        addOption(options, new Option(ARG.entityName.getArgName(), true,
                "name of the entity"));
        addOption(options, new Option(ARG.entityType.getArgName(), true,
                "type of the entity"));
        addOption(options, new Option(ARG.feedInstancePaths.getArgName(),
                true, "feed instance paths"));
        addOption(options, new Option(ARG.feedNames.getArgName(), true,
                "feed names"));
        addOption(options, new Option(ARG.logFile.getArgName(), true,
                "log file path"));
        addOption(options, new Option(ARG.nominalTime.getArgName(), true,
                "instance time"));
        addOption(options, new Option(ARG.operation.getArgName(), true,
                "operation like generate, delete, archive"));
        addOption(options, new Option(ARG.runId.getArgName(), true,
                "current run-id of the instance"));
        addOption(options, new Option(ARG.status.getArgName(), true,
                "status of workflow instance"));
        addOption(options, new Option(ARG.timeStamp.getArgName(), true,
                "current timestamp"));
        addOption(options, new Option(ARG.topicName.getArgName(), true,
                "name of the topic to be used to send message"));
        addOption(options, new Option(ARG.workflowId.getArgName(), true,
                "workflow id"));
        addOption(options, new Option(ARG.cluster.getArgName(), true,
                "cluster name"));
        addOption(options, new Option(ARG.workflowUser.getArgName(), true,
                "workflow user id"), false);
        addOption(options, new Option(ARG.logDir.getArgName(), true,
                "log dir where job logs are copied"), false);

        return new GnuParser().parse(options, arguments);
    }

    private static void addOption(Options options, Option opt) {
        addOption(options, opt, true);
    }

    private static void addOption(Options options, Option opt, boolean isRequired) {
        opt.setRequired(isRequired);
        options.addOption(opt);
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLine cmd;
        try {
            cmd = getCommand(args);
        } catch (ParseException e) {
            throw new Exception("Unable to parse arguments: ", e);
        }
        List<EntityInstanceMessage> entityInstanceMessage = EntityInstanceMessage
                .getMessages(cmd);
        if (entityInstanceMessage == null || entityInstanceMessage.isEmpty()) {
            LOG.warn("No operation on output feed");
            return 0;
        }

        try {
            createAndStartConnection(cmd.getOptionValue(ARG.brokerImplClass.name()), "",
                    "", cmd.getOptionValue(ARG.brokerUrl.name()));
            for (EntityInstanceMessage message : entityInstanceMessage) {
                LOG.info("Sending message: {}", message.getKeyValueMap());
                sendMessage(message);
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (JMSException e) {
                LOG.error("Error in closing connection", e);
            }
        }
        return 0;
    }
}
