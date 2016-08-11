package com.nexr.lean.kafka;

import kafka.common.TopicAlreadyMarkedForDeletionException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicManagerTest {

    private static Logger log = LoggerFactory.getLogger(KafkaTopicManagerTest.class);

    private static String zkServers = null;

    @BeforeClass
    public static void setupClass() {
        try {
            KafkaProxyTestServers.startServers();
        } catch (Exception e) {
            log.warn("Fail to initialize the local kafka, local zookeeper for testing");
            Assert.fail();
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            KafkaProxyTestServers.shutdownServers();
        } catch (Exception e) {
            log.warn("Fail to shutdown the local kafka, local zookeeper for testing");
        }
    }

    @Test
    public void testManageTopic() throws InterruptedException {
        String topic = "az-test";
        int partitions = 5;
        int replications = 1;

        Assert.assertEquals(0, KafkaTopicManager.listTopics(zkServers).size());

        // initial check
        Assert.assertEquals(false, KafkaTopicManager.topicExists(zkServers, topic));

        // Create topic
        KafkaTopicManager.createTopic(zkServers, topic, partitions, replications);

        // Check exists
        Assert.assertEquals(true, KafkaTopicManager.topicExists(zkServers, topic));

        // List up Topics. in this case retrieved topic count is 1.
        Assert.assertEquals(1, KafkaTopicManager.listTopics(zkServers).size());

        // TODO checking log ? Already exists
        KafkaTopicManager.createTopic(zkServers, topic, partitions, replications);

        Assert.assertEquals(true, KafkaTopicManager.topicExists(zkServers, topic));
        Thread.sleep(1000);

        // Delete topic
        KafkaTopicManager.deleteTopic(zkServers, topic);
        Thread.sleep(1000);

        if (KafkaTopicManager.topicExists(zkServers, topic)) {
            log.info("Topic still exists, try again");
            try {
                KafkaTopicManager.deleteTopic(zkServers, topic);
                Assert.fail("Topic should be deleted or marked for deletion");
            } catch (TopicAlreadyMarkedForDeletionException e) {
                log.info("Topic is marked for deletion");
            }
        }
    }

    static {
        zkServers = "localhost:" + KafkaProxyTestServers.ZK_PORT;
    }

}
