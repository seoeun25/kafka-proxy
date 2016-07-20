package com.nexr.lean.kafka.util;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Local Zookeeper Server for testing.
 */
public class LocalZKServer {

    private final static Logger log = LoggerFactory.getLogger(LocalZKServer.class);

    private static final int DEFAULT_PORT = 3181;

    private final TestingServer server;

    /**
     * Create zookeeper server with port 3181.
     *
     * @throws Exception
     */
    public LocalZKServer() throws Exception {
        this(DEFAULT_PORT);
    }

    public LocalZKServer(int port) throws Exception {
        this.server = new TestingServer(port);
    }

    public void stop() throws IOException {
        log.debug("Shutting down embedded ZooKeeper server at {} ...", server.getConnectString());
        server.close();
        log.info("Shutdown of embedded ZooKeeper server at {} completed", server.getConnectString());
    }

    /**
     * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
     * Example: `127.0.0.1:2181`.
     * <p/>
     * You can use this to e.g. tell Kafka brokers how to connect to this instance.
     */
    public String getConnectString() {
        return server.getConnectString();
    }

    /**
     * The hostname of the ZooKeeper instance.  Example: `127.0.0.1`
     */
    public String hostname() {
        // "server:1:2:3" -> "server:1:2"
        return getConnectString().substring(0, getConnectString().lastIndexOf(':'));
    }
}
