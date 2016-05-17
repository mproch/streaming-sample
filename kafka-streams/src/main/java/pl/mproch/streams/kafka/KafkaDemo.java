package pl.mproch.streams.kafka;

import org.apache.kafka.streams.kstream.KTable;
import pl.mproch.streaming.model.User;
import pl.mproch.streaming.model.Message;


import static org.apache.kafka.common.serialization.Serdes.String;
import static pl.mproch.streaming.model.Topics.*;
import static pl.mproch.streaming.model.InternalTopics.*;

public class KafkaDemo extends KafkaDemoRunner {

    public static void main(String[] args) throws Exception {
        new KafkaDemo().runStreams(builder -> {
            KTable<String, User> usersStream = builder
                    .table(String(), mapperSerializer(User.class), users.name());

        });

        //highRankUsers
        //messageCount
        //averageRateByText
        //lowRatingUsers
    }
}
