package com.nexr.lean.kafka.util;

import com.nexr.lean.kafka.common.ConfigUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Local instance of the Kafka broker. For testing.
 */
public final class LocalKafkaBroker implements Closeable {

    static final int TEST_BROKER_ID = 0;

    private static final Logger log = LoggerFactory.getLogger(LocalKafkaBroker.class);

    private final int port;
    private final int zkPort;
    private Path logsDir;
    private KafkaServerStartable kafkaServer;

    /**
     * Creates an instance that will listen on the given port and connect to the given
     * Zookeeper port.
     *
     * @param port   port for Kafka broker to listen on
     * @param zkPort port on which Zookeeper is listening
     */
    public LocalKafkaBroker(int port, int zkPort) {
        this.port = port;
        this.zkPort = zkPort;
    }

    public static void deleteRecursively(Path rootPath) {
        if (rootPath == null || !Files.exists(rootPath)) {
            return;
        }

        try {
            Files.walkFileTree(rootPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                        throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getPort() {
        return port;
    }

    /**
     * Starts the Kafka broker.
     *
     * @throws IOException if an error occurs during initialization
     */
    public synchronized void start() throws IOException {
        log.info(" Starting Kafka broker on port {} !!", port);

        logsDir = Files.createTempDirectory(LocalKafkaBroker.class.getSimpleName());
        logsDir.toFile().deleteOnExit();
        kafkaServer = new KafkaServerStartable(new KafkaConfig(ConfigUtils.keyValueToProperties(
                "broker.id", TEST_BROKER_ID,
                "log.dirs", logsDir.toAbsolutePath(),
                "port", port,
                "zookeeper.connect", "localhost:" + zkPort,
                "num.partitions", "2"
        ), false));
        kafkaServer.startup();
        log.info(" Starting Kafka broker on dir {} , {} !!", logsDir.getParent(), logsDir.getFileName());
    }

    /**
     * Blocks until the Kafka broker terminates.
     */
    public void await() {
        kafkaServer.awaitShutdown();
    }

    /**
     * Stops the Kafka broker.
     */
    @Override
    public synchronized void close() throws IOException {
        log.info("Closing...");
        if (kafkaServer != null) {
            kafkaServer.shutdown();
            kafkaServer.awaitShutdown();
            kafkaServer = null;
        }
        if (logsDir != null) {
            LocalKafkaBroker.deleteRecursively(logsDir);
            logsDir = null;
        }
    }

}
