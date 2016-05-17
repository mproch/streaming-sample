package pl.mproch.streams.kafka;

import java.util.Optional;
import org.apache.kafka.streams.kstream.KTable;
import pl.mproch.streaming.model.Message;
import pl.mproch.streaming.model.User;

import static org.apache.kafka.common.serialization.Serdes.String;
import static pl.mproch.streaming.model.Topics.highRankUsers;
import static pl.mproch.streaming.model.Topics.messages;
import static pl.mproch.streaming.model.Topics.users;


public class KafkaJoinDemoReady extends KafkaDemoRunner {

    public static void main(String[] args) throws Exception {
        new KafkaJoinDemoReady().runStreams(builder -> {
            KTable<String, User> usersStream = builder
                    .table(String(), mapperSerializer(User.class), users.name());


            builder.stream(String(), mapperSerializer(Message.class), messages.name())
                    .leftJoin(usersStream, Message::withUser)
                    .filter((k, v) -> Optional.ofNullable(v.getUser()).filter(u -> u.getRank() > 10).isPresent())
                    .to(String(), mapperSerializer(Message.class), highRankUsers.name());

        });
    }
}
