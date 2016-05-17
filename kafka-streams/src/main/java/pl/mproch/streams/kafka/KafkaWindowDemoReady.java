package pl.mproch.streams.kafka;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TumblingWindows;
import pl.mproch.streaming.model.AverageRate;
import pl.mproch.streaming.model.Message;

import static org.apache.kafka.common.serialization.Serdes.String;
import static pl.mproch.streaming.model.Topics.*;
import static pl.mproch.streaming.model.InternalTopics.*;

public class KafkaWindowDemoReady extends KafkaDemoRunner {

    public static void main(String[] args) throws Exception {

        new KafkaWindowDemoReady().runStreams((builder) -> {


            KStream<String, Message> messagesStream =
                    builder.stream(String(), mapperSerializer(Message.class), messages.name());


            messagesStream
                    .map((k, v) -> new KeyValue<>(v.getTopic(), v.getRate()))
                    .aggregateByKey(() -> new AverageRate(0, 0), (k, rate, ag) -> ag.add(rate),
                            TumblingWindows.of(averageRate.name()).with(10000), String(), mapperSerializer(AverageRate.class))
                    .toStream()
                    .mapValues(AverageRate::computeRate)
                    .to(Windowed(), DoubleStr(), averageRateByText.name());

            messagesStream
                    .mapValues(Message::getRate)
                    .aggregateByKey(() -> new AverageRate(0, 0), (k, rate, ag) -> ag.add(rate),
                            HoppingWindows.of(averageHoppingRate.name()).with(30000).every(10000),
                            String(), mapperSerializer(AverageRate.class))
                    .toStream()
                    .mapValues(AverageRate::computeRate)
                    .filter((k, v) -> v < 5)
                    .to(Windowed(), DoubleStr(), lowRatingUsers.name());
        });


    }


}
