package com.nexr.lean.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * This class provide local zookeeper and local kafka for testing.
 */
public class TestServers {

    public static final int ZK_PORT = 3181;
    public static final int BROKER_PORT = 19092;
    private static Logger log = LoggerFactory.getLogger(TestServers.class);
    private static LocalZKServer zookeeperServer;
    private static LocalKafkaBroker kafkaBroker;

    public static void startServers() throws InterruptedException {
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    zookeeperServer = new LocalZKServer(ZK_PORT);
                    log.info(" zookeeper connection String {} !!", zookeeperServer.getConnectString());
                    zookeeperServer.getConnectString();
                    kafkaBroker = new LocalKafkaBroker(BROKER_PORT, ZK_PORT);
                    kafkaBroker.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();
        Thread.sleep(3000);
    }

    public static void shutdownServers() throws Exception {
        if (kafkaBroker != null) {
            kafkaBroker.close();
        }
        Thread.sleep(1000);
        if (zookeeperServer != null) {
            zookeeperServer.stop();
        }
    }

    /**
     * Returns the properties for testing.
     *
     * @return a properties contain the configurations for testing.
     */
    public static Properties getPropertiesForTesting() {
        // TODO The context for the testing could work with IOC container such as guice, spring and so on.
        // Currently, KafkaProxy does not use any IOC container, so that it make context manually.
        TestServers.class.getResourceAsStream("test.properties");
        Properties properties = new Properties();
        try {
            properties.load(TestServers.class.getResourceAsStream("/test.properties"));
            for (Object key : properties.keySet()) {
                log.debug("testing properties : {} = {}", key.toString(), properties.getProperty(key.toString()));
            }
            String zkServers, brokers, schemaRegistryClass, schemaRegistryUrl;
            if (properties.containsKey("test.method") && properties.getProperty("test.method").equals("integration-test")) {
                zkServers = properties.getProperty("test.zkserver", "localhost:2181");
                brokers = properties.getProperty("test.brokers", "localhost:9092");
                schemaRegistryClass = properties.getProperty("test" + ".schemaregistry.class", "com.nexr.schemaregistry.SimpleSchemaRegistryClient");
                schemaRegistryUrl = properties.getProperty("test.schemaregistry.url", "http://localhost:18181/repo");

            } else {
                properties.put("test.method", "unit-test");
                zkServers = "localhost:" + TestServers.ZK_PORT;
                brokers = "localhost:" + TestServers.BROKER_PORT;
                schemaRegistryClass = "com.nexr.lean.kafka.util.DummySchemaRegistryClient";
                schemaRegistryUrl = "http://hello:18181/repo";
            }
            properties.put("zkServers", zkServers);
            properties.put("brokers", brokers);
            properties.put("schemaRegistryUrl", schemaRegistryUrl);
            properties.put("schemaRegistryClass", schemaRegistryClass);
        } catch (Exception e) {
            log.warn("fail to load the properties for testing : " + e.getMessage());
        }
        return properties;
    }

}
