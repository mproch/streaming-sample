package pl.mproch.streaming.common.run;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.ZkUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import scala.Some;

public class RunDemo {

    public static void main(String[] args) throws Exception {
        new RunDemo(2181, 9092);

        Arrays.asList(
                "messages", "users",
                "keyedMessages", "userIdMessages",
                "messageAmount", "averageRate",
                "averageHoppingRate", "usersPostingMessage")
            .forEach(RunDemo::createTopic);

        new MessageProducer();

    }

    private static void createTopic(String name) {
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 20000, 20000, false);
        AdminUtils.createTopic(zkUtils, name, 1, 1, new Properties(), null);
    }

    RunDemo(int zkPort, int kafkaPort) throws Exception {
        runZookeeper(zkPort);
        runKafka(zkPort, kafkaPort);
    }

    ServerCnxnFactory runZookeeper(int zkPort) throws Exception {
        NIOServerCnxnFactory factory = new NIOServerCnxnFactory();
        factory.configure(new InetSocketAddress("127.0.0.1", zkPort), 1024);
        ZooKeeperServer zkServer = new ZooKeeperServer(Files.createTempDirectory("zk1").toFile(), Files.createTempDirectory("zk1").toFile(), 100);
        factory.startup(zkServer);
        return factory;
    }

    KafkaServer runKafka(int zkPort, int kafkaPort) throws IOException {

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "127.0.0.1:" + zkPort);
        properties.setProperty("broker.id", "0");
        properties.setProperty("host.name", "127.0.0.1");
        properties.setProperty("hostname", "127.0.0.1");
        properties.setProperty("advertised.host.name", "127.0.0.1");
        properties.setProperty("num.partitions", "1");
        properties.setProperty("auto.create.topics.enable", "true");

        properties.setProperty("port", "" + kafkaPort);
        properties.setProperty("log.dir", Files.createTempDirectory("kafka-logs").toFile().getAbsolutePath());

        KafkaServer server = new KafkaServer(new KafkaConfig(properties, false), SystemTime$.MODULE$, new Some("thread"));
        server.startup();

        return server;
    }


}
