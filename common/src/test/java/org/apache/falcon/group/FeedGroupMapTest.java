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
package org.apache.falcon.group;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.util.Map;

/**
 * Feed group map tests.
 */
public class FeedGroupMapTest extends AbstractTestBase {
    private static Cluster cluster;

    @BeforeClass
    public void setUp() throws Exception {
        cluster = (Cluster) EntityType.CLUSTER
                .getUnmarshaller()
                .unmarshal(
                        FeedGroupMapTest.class
                                .getResourceAsStream("/config/cluster/cluster-0.1.xml"));
    }

    @BeforeMethod
    public void cleanup() throws Exception {
        cleanupStore();
    }

    @Test
    public void testOnAdd() throws FalconException, JAXBException {
        getStore().publish(EntityType.CLUSTER, cluster);
        Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));
        feed1.setName("f1");
        feed1.setGroups("group1,group2,group3");
        Location location = new Location();
        location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
        location.setType(LocationType.DATA);
        feed1.setLocations(new Locations());
        feed1.getLocations().getLocations().add(location);
        getStore().publish(EntityType.FEED, feed1);
        Map<String, FeedGroup> groupMapping = FeedGroupMap.get()
                .getGroupsMapping();

        FeedGroup group = groupMapping.get("group1");
        Assert.assertEquals(group.getName(), "group1");
        Assert.assertEquals(group.getFeeds().size(), 1);
        assertFields(group, feed1);

        group = groupMapping.get("group2");
        Assert.assertEquals(group.getName(), "group2");
        Assert.assertEquals(group.getFeeds().size(), 1);
        assertFields(group, feed1);

        group = groupMapping.get("group3");
        Assert.assertEquals(group.getName(), "group3");
        Assert.assertEquals(group.getFeeds().size(), 1);
        assertFields(group, feed1);

        Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));

        feed2.setName("f2");
        feed2.setGroups("group1,group5,group3");
        location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/${MONTH}/${DAY}/ad2");
        location.setType(LocationType.DATA);
        feed2.setLocations(new Locations());
        feed2.getLocations().getLocations().add(location);
        getStore().publish(EntityType.FEED, feed2);
        groupMapping = FeedGroupMap.get().getGroupsMapping();

        group = groupMapping.get("group1");
        Assert.assertEquals(group.getName(), "group1");
        Assert.assertEquals(group.getFeeds().size(), 2);
        assertFields(group, feed2);

        group = groupMapping.get("group2");
        Assert.assertEquals(group.getName(), "group2");
        Assert.assertEquals(group.getFeeds().size(), 1);
        assertFields(group, feed2);

        group = groupMapping.get("group3");
        Assert.assertEquals(group.getName(), "group3");
        Assert.assertEquals(group.getFeeds().size(), 2);
        assertFields(group, feed2);

        group = groupMapping.get("group5");
        Assert.assertEquals(group.getName(), "group5");
        Assert.assertEquals(group.getFeeds().size(), 1);
        assertFields(group, feed2);

    }

    @Test
    public void testOnRemove() throws FalconException, JAXBException {
        Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));
        feed1.setName("f1");
        getStore().publish(EntityType.CLUSTER, cluster);
        feed1.setGroups("group7,group8,group9");
        Location location = new Location();
        location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
        location.setType(LocationType.DATA);
        feed1.setLocations(new Locations());
        feed1.getLocations().getLocations().add(location);
        getStore().publish(EntityType.FEED, feed1);

        Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));
        feed2.setName("f2");
        feed2.setGroups("group7,group8,group10");
        location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/${MONTH}/${DAY}/ad2");
        location.setType(LocationType.DATA);
        feed2.setLocations(new Locations());
        feed2.getLocations().getLocations().add(location);
        getStore().publish(EntityType.FEED, feed2);

        Map<String, FeedGroup> groupMapping = FeedGroupMap.get()
                .getGroupsMapping();

        getStore().remove(EntityType.FEED, "f2");

        FeedGroup group = groupMapping.get("group7");
        Assert.assertEquals(group.getName(), "group7");
        Assert.assertEquals(group.getFeeds().size(), 1);

        group = groupMapping.get("group8");
        Assert.assertEquals(group.getName(), "group8");
        Assert.assertEquals(group.getFeeds().size(), 1);

        group = groupMapping.get("group10");
        Assert.assertEquals(null, group);

        getStore().remove(EntityType.FEED, "f1");

        group = groupMapping.get("group7");
        Assert.assertEquals(null, group);

        group = groupMapping.get("group8");
        Assert.assertEquals(null, group);

        group = groupMapping.get("group9");
        Assert.assertEquals(null, group);

    }

    @Test
    public void testNullGroup() throws FalconException, JAXBException {
        Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));
        feed1.setName("f5" + System.currentTimeMillis());
        getStore().publish(EntityType.CLUSTER, cluster);
        feed1.setGroups(null);
        Location location = new Location();
        location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
        location.setType(LocationType.DATA);
        feed1.setLocations(new Locations());
        feed1.getLocations().getLocations().add(location);
        getStore().publish(EntityType.FEED, feed1);

    }

    private void assertFields(FeedGroup group, Feed feed) {
        Assert.assertEquals(group.getFrequency(), feed.getFrequency());
        Assert.assertEquals(group.getDatePattern(),
                "[${DAY}, ${MONTH}, ${YEAR}]");
    }
}
