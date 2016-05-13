package pl.mproch.streams.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Windowed;

abstract class KafkaDemo {

    void runStreams(Consumer<KStreamBuilder> action) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, StringSerde.class);

        KStreamBuilder builder = new KStreamBuilder();
        action.accept(builder);
        KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(props));

        streams.start();

        Thread.currentThread().join();
        streams.close();
    }

    static <K> Serde<K> mapperSerializer(Class<K> klass) {
        return new Serde<K>() {
            private ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public Serializer<K> serializer() {
                return new SimpleSerializer<>((topic, data) -> {
                    try {
                        if (data == null) return null;
                        return objectMapper.writeValueAsBytes(data);
                    } catch (JsonProcessingException e) {
                        throw new SerializationException("Failed to serialize", e);
                    }
                });
            }

            @Override
            public Deserializer<K> deserializer() {
                return new SimpleDeserializer<>((topic, data) -> {
                    try {
                        if (data == null) return null;
                        return objectMapper.readValue(data, klass);
                    } catch (IOException e) {
                        throw new DeserializationException("Failed to read", e);
                    }
                });
            }
        };


    }

    static Serde<Double> DoubleStr() {
        return OnlySerializing((topic, doub) -> ("" + doub).getBytes());
    }

    static Serde<Long> LongStr() {
        return OnlySerializing((topic, doub) -> ("" + doub).getBytes());
    }

    private static String localTime(long time) {
        return LocalDateTime.ofInstant(new Date(time).toInstant(), ZoneId.systemDefault()).toLocalTime().toString();
    }

    static <T> Serde<Windowed<T>> Windowed() {
        return OnlySerializing((topic, window) ->
                String.format("[%s-%s]: %s", localTime(window.window().start()),
                        localTime(window.window().end()), window.value()).getBytes());
    }

    private static class SimpleSerializer<T> implements Serializer<T> {

        private BiFunction<String, T, byte[]> fn;

        private SimpleSerializer(BiFunction<String, T, byte[]> fn) {
            this.fn = fn;
        }


        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public void close() {
        }

        @Override
        public byte[] serialize(String topic, T data) {
            return fn.apply(topic, data);
        }
    }

    private static class SimpleDeserializer<T> implements Deserializer<T> {

        private BiFunction<String, byte[], T> fn;

        public SimpleDeserializer(BiFunction<String, byte[], T> fn) {
            this.fn = fn;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public void close() {
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            return fn.apply(topic, data);
        }
    }

    private static <T> Serde<T> OnlySerializing(BiFunction<String, T, byte[]> fn) {

        return new Serde<T>() {
            @Override
            public Serializer<T> serializer() {
                return new SimpleSerializer<>(fn);
            }

            @Override
            public Deserializer<T> deserializer() {
                return null;
            }
        };

    }
}
