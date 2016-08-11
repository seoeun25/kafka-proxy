package com.nexr.lean.kafka;

import com.nexr.lean.kafka.util.LocalKafkaBroker;
import com.nexr.lean.kafka.util.LocalZKServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provide local zookeeper and local kafka for testing.
 */
public class KafkaProxyTestServers {

    public static final int ZK_PORT = 3181;
    public static final int BROKER_PORT = 19092;
    private static Logger log = LoggerFactory.getLogger(KafkaProxyTestServers.class);
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

}
