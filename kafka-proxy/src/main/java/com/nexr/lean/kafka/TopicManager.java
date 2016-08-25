package com.nexr.lean.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicExistsException;
import kafka.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TopicManager {

    private static final int ZK_TIMEOUT = (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
    private static Logger log = LoggerFactory.getLogger(TopicManager.class);

    public TopicManager() {

    }

    /**
     * List topics
     *
     * @param zkServers
     * @return List<String> entire topic list
     */
    public static List<String> listTopics(String zkServers) {
        ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT, ZK_TIMEOUT, false);
        List<String> allTopics = new ArrayList<>();

        try {
            zkUtils.getAllTopics();
            allTopics.addAll(JavaConversions.asJavaList(zkUtils.getAllTopics()));
            log.debug("list topic {} ", allTopics);
        } finally {
            zkUtils.close();
        }

        return allTopics;
    }

    /**
     * Check topic exists or not.
     *
     * @param zkServers Zookeeper server string. ex> host1:port1,host2:port2,...
     * @param topic     topic to check
     * @return {@code true} if topic exists
     */
    public static boolean topicExists(String zkServers, String topic) {
        ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT, ZK_TIMEOUT, false);
        try {
            return AdminUtils.topicExists(zkUtils, topic);
        } finally {
            zkUtils.close();
        }
    }

    /**
     * Create topic.
     *
     * @param zkServers    Zookeeper server string. ex> host1:port1,host2:port2,...
     * @param topic        topic to create
     * @param partitions   number of topic partitions
     * @param replications number of replication factors
     */
    public static void createTopic(String zkServers, String topic, int partitions, int replications) {
        ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT, ZK_TIMEOUT, false);
        try {
            AdminUtils.createTopic(zkUtils, topic, partitions, replications, new Properties(), RackAwareMode.Enforced$.MODULE$);
            log.info("Created topic {} ", topic);
        } catch (TopicExistsException e) {
            log.info("Topic {} already exists.", topic);
        } finally {
            zkUtils.close();
        }
    }

    /**
     * Delete topic.
     *
     * @param zkServers Zookeeper server string. ex> host1:port1,host2:port2,...
     * @param topic     topic to delete
     */
    public static void deleteTopic(String zkServers, String topic) {
        ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT, ZK_TIMEOUT, false);
        try {
            if (AdminUtils.topicExists(zkUtils, topic)) {
                AdminUtils.deleteTopic(zkUtils, topic);
                log.info("Deleted topic {} ", topic);
            } else {
                log.info("No need to delete topic {}. It does not exist.");
            }
        } finally {
            zkUtils.close();
        }
    }

}
