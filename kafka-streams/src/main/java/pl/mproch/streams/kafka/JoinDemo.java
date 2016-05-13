package pl.mproch.streams.kafka;

import java.util.Optional;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import pl.mproch.streaming.model.Message;
import pl.mproch.streaming.model.User;

import static org.apache.kafka.common.serialization.Serdes.String;

public class JoinDemo extends KafkaDemo {

    public static void main(String[] args) throws Exception {
        new JoinDemo().runStreams(builder -> {
            KTable<String, User> users = builder
                    .table(String(), mapperSerializer(User.class), "users");


            builder.stream(String(), mapperSerializer(Message.class), "messages")
                    .map((k, v) -> new KeyValue<>(v.getUserId(), v))
                    .through(String(), mapperSerializer(Message.class), "keyedMessages")
                    .leftJoin(users, Message::withUser)
                    .filter((k, v) -> Optional.ofNullable(v.getUser()).filter(u -> u.getRank() > 10).isPresent())
                    .to(String(), mapperSerializer(Message.class), "highRankUsers");

        });
    }
}
