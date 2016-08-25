package com.nexr.lean.kafka;

import com.nexr.lean.kafka.util.TestServers;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TopicManagerTest {

    private static Logger log = LoggerFactory.getLogger(TopicManagerTest.class);

    private static String zkServers = null;

    @BeforeClass
    public static void setupClass() {
        try {
            TestServers.startServers();
        } catch (Exception e) {
            log.warn("Fail to initialize the local kafka, local zookeeper for testing");
            Assert.fail();
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            TestServers.shutdownServers();
        } catch (Exception e) {
            log.warn("Fail to shutdown the local kafka, local zookeeper for testing");
        }
    }

    @Test
    public void testManageTopic() throws InterruptedException {
        String topic = "az-test";
        int partitions = 5;
        int replications = 1;

        // initial check
        Assert.assertEquals(false, TopicManager.topicExists(zkServers, topic));

        // Create topic
        TopicManager.createTopic(zkServers, topic, partitions, replications);

        // Check exists
        Assert.assertEquals(true, TopicManager.topicExists(zkServers, topic));

        // List up Topics.
        Assert.assertTrue(TopicManager.listTopics(zkServers).size() > 0);

        // TODO checking log ? Already exists
        TopicManager.createTopic(zkServers, topic, partitions, replications);

        Assert.assertEquals(true, TopicManager.topicExists(zkServers, topic));
        Thread.sleep(500);

        // Delete topic
        TopicManager.deleteTopic(zkServers, topic);
        Thread.sleep(1000);

        if (TopicManager.topicExists(zkServers, topic)) {
            log.info("Topic still exists, try again");
            try {
                TopicManager.deleteTopic(zkServers, topic);
                Assert.fail("Topic should be deleted or marked for deletion");
            } catch (TopicAlreadyMarkedForDeletionException e) {
                log.info("Topic is marked for deletion");
            }
        }
    }

    static {
        zkServers = "localhost:" + TestServers.ZK_PORT;
    }

}
