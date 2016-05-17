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
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.mproch.streaming.model.Topics;

public class MessageConsumer {

    public static void main(String[] args) throws IOException {
        new MessageConsumer();
    }

    private ObjectMapper objectMapper = new ObjectMapper();

    private Map<String, List<String>> topicsMap = new HashMap<>();

    private Consumer<String, String> consumer;

    private volatile boolean running = true;

    private HttpServer httpServer;

    public MessageConsumer() throws IOException {
        Stream.of(Topics.values()).forEach(t -> topicsMap.put(t.name(), new CopyOnWriteArrayList<>()));
        consumer = prepareConsumer();
        consumer.subscribe(topicsMap.keySet());
        new Thread(() -> {
            while (running) {
                consumer.poll(100).forEach(record -> {
                    String keyPrint = record.key() == null ? "" : (", key: " + record.key());
                    System.out.println(
                            Topics.valueOf(record.topic()).color +
                            "Topic: " + record.topic() + keyPrint + ", value: " + record.value()
                            + "\u001B[0m");
                    topicsMap.get(record.topic()).add(record.value());
                });
            }
            consumer.close();
        }).start();
        httpServer = HttpServer.create();
        httpServer.bind(new InetSocketAddress("localhost", 8080), 0);
        topicsMap.keySet().forEach(t ->
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
        props.put("auto.offset.reset", "latest");


        return new KafkaConsumer<>(props);
    }
}
