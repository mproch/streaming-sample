package pl.mproch.streams.kafka;

import org.apache.kafka.streams.kstream.KStream;
import pl.mproch.streaming.model.Message;

import static org.apache.kafka.common.serialization.Serdes.String;
import static pl.mproch.streaming.model.InternalTopics.messageAmount;
import static pl.mproch.streaming.model.Topics.messageCount;
import static pl.mproch.streaming.model.Topics.messages;

public class KafkaStateDemoReady extends KafkaDemoRunner {

    public static void main(String[] args) throws Exception {
        new KafkaStateDemoReady().runStreams(builder -> {
            KStream<String, Message> messagesStream =
                    builder.stream(String(), mapperSerializer(Message.class), messages.name());

            messagesStream
                    .countByKey(messageAmount.name())
                    .toStream()
                    .to(String(), LongStr(), messageCount.name());

        });
    }
}
