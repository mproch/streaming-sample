package pl.mproch.streaming.common.run;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.mproch.streaming.model.Message;
import pl.mproch.streaming.model.User;

public class MessageProducer {

    private ObjectMapper objectMapper = new ObjectMapper();

    private Random random = new Random();

    public MessageProducer() {
        KafkaProducer<String, byte[]> producer = prepareProducer();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(() ->
                        producer.send(generateMessage()), 1500, 1500, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(() ->
                        producer.send(generateUser()), 5000, 5000, TimeUnit.MILLISECONDS);
    }

    private List<String> randomMessages = Arrays.asList(
            "JEEConf", "Kafka Streams", "Apache Flink", "Hadoop", "Kiev");


    private ProducerRecord<String, byte[]> generateMessage() {
        try {
            String userId = "user" + random.nextInt(5);
            return new ProducerRecord<>("messages",
                    objectMapper.writeValueAsBytes(
                            new Message(System.currentTimeMillis(),
                                    userId, randomMessages.get(random.nextInt(5)), random.nextInt(10))));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to generate message");
        }
    }

    private ProducerRecord<String, byte[]> generateUser() {
        try {
            String userId = "user" + random.nextInt(5);
            return new ProducerRecord<>("users", userId,
                    objectMapper.writeValueAsBytes(
                            new User(userId, "Name", random.nextInt(20), random.nextBoolean() ? 15 : 20)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to generate message");
        }
    }


    public static KafkaProducer<String, byte[]> prepareProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("batch.size", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        return new KafkaProducer<>(props);
    }


}
