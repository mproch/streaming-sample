package pl.mproch.streaming.common.run;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageConsumer {

    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public static void main(String[] args) throws IOException {
        Map<String, String> topics = new HashMap<>();

        topics.put("highRankUsers", ANSI_CYAN);
        topics.put("messageCount", ANSI_RED);
        topics.put("timedMessageCount", ANSI_GREEN);
        topics.put("averageRateByText", ANSI_YELLOW);
        topics.put("lowRatingUsers", ANSI_BLUE);
        topics.put("lowRankedMessages", ANSI_PURPLE);

        topics.put("messages", ANSI_BLACK);
        topics.put("users", ANSI_BLACK);
        topics.put("usersPostingMessage", ANSI_CYAN);

        new MessageConsumer(topics);
    }

    private ObjectMapper objectMapper = new ObjectMapper();

    private Map<String, List<String>> topicsMap = new HashMap<>();

    private Consumer<String, String> consumer;

    private volatile boolean running = true;

    private HttpServer httpServer;

    public MessageConsumer(final Map<String, String> topics) throws IOException {
        topics.keySet().forEach(t -> topicsMap.put(t, new CopyOnWriteArrayList<>()));
        consumer = prepareConsumer();
        consumer.subscribe(topics.keySet());
        new Thread(() -> {
            while (running) {
                consumer.poll(100).forEach(record -> {
                    String keyPrint = record.key() == null ? "" : (", key: " + record.key());
                    System.out.println(
                            topics.get(record.topic()) +
                            "Topic: " + record.topic() + keyPrint + ", value: " + record.value()
                            + "\u001B[0m");
                    topicsMap.get(record.topic()).add(record.value());
                });
            }
            consumer.close();
        }).start();
        httpServer = HttpServer.create();
        httpServer.bind(new InetSocketAddress("localhost", 8080), 0);
        topics.keySet().forEach(t ->
        httpServer.createContext("/" + t).setHandler((ex) -> {
            OutputStream responseBody = ex.getResponseBody();
            byte[] answer = objectMapper.writeValueAsBytes(topicsMap.get(t));
            ex.getResponseHeaders().add("Content-Type", "application/json");

            ex.sendResponseHeaders(200, answer.length);
            responseBody.write(answer);
            responseBody.flush();
        }));
        httpServer.start();
    }

    private static KafkaConsumer<String, String> prepareConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");
        props.put("auto.commit.interval.ms", "1000");
        props.put("enable.auto.commit", "true");

        return new KafkaConsumer<>(props);
    }
}
