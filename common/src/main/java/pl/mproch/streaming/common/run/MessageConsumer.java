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

    private ObjectMapper objectMapper = new ObjectMapper();

    private Map<String, List<String>> topicsMap = new HashMap<>();

    private Consumer<String, String> consumer;

    private volatile boolean running = true;

    private HttpServer httpServer;

    public MessageConsumer(final List<String> topics) throws IOException {
        topics.forEach(t -> topicsMap.put(t, new CopyOnWriteArrayList<>()));
        consumer = prepareConsumer();
        consumer.subscribe(topics);
        new Thread(() -> {
            while (running) {
                consumer.poll(100).forEach(record -> {
                    String keyPrint = record.key() == null ? "" : (", key: " + record.key());
                    System.out.println("Topic: " + record.topic() + keyPrint + ", value: " + record.value());
                    topicsMap.get(record.topic()).add(record.value());
                });
            }
            consumer.close();
        }).start();
        httpServer = HttpServer.create();
        httpServer.bind(new InetSocketAddress("localhost", 8080), 0);
        topics.forEach(t ->
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

    void stop() {
        running = false;
        httpServer.stop(0);
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
