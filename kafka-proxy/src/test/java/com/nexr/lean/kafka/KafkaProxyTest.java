package com.nexr.lean.kafka;

import com.nexr.lean.kafka.util.LocalKafkaBroker;
import com.nexr.lean.kafka.util.LocalZKServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provide local zookeeper and local kafka for testing.
 */
public abstract class KafkaProxyTest {

    private static Logger log = LoggerFactory.getLogger(KafkaProxyTest.class);

    private static LocalZKServer zookeeperServer;
    private static LocalKafkaBroker kafkaBroker;

    protected static int ZK_PORT = 3181;
    protected static int  BROKER_PORT = 19092;

    @BeforeClass
    public static void setupClass() {
        try {
            startServers();
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            if (kafkaBroker != null) {
                kafkaBroker.close();
            }
            Thread.sleep(1000);
            if (zookeeperServer != null) {
                zookeeperServer.stop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startServers() throws InterruptedException {
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    zookeeperServer = new LocalZKServer(ZK_PORT);
                    log.info("zookeeper connection String {} ", zookeeperServer.getConnectString());
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

}
