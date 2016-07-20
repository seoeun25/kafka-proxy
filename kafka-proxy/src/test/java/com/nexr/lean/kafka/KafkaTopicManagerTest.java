package com.nexr.lean.kafka;

import kafka.common.TopicAlreadyMarkedForDeletionException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicManagerTest extends KafkaProxyTest{

    private static Logger log = LoggerFactory.getLogger(KafkaTopicManagerTest.class);

    private static String zkServers = null;

    static {
        zkServers = "localhost:" + ZK_PORT;
    }

    @Test
    public void testManageTopic() throws InterruptedException {
        String topic = "az-test";
        int partitions = 5;
        int replications = 1;

        // initial check
        Assert.assertEquals(false, KafkaTopicManager.topicExists(zkServers, topic));

        // Create topic
        KafkaTopicManager.createTopic(zkServers, topic, partitions, replications);

        // Check exists
        Assert.assertEquals(true, KafkaTopicManager.topicExists(zkServers, topic));

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

}
