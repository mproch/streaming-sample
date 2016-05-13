package pl.mproch.streams.kafka;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TumblingWindows;
import pl.mproch.streaming.model.AverageRate;
import pl.mproch.streaming.model.Message;

import static org.apache.kafka.common.serialization.Serdes.String;

public class WindowDemo extends KafkaDemo {

    public static void main(String[] args) throws Exception {

        new WindowDemo().runStreams((builder) -> {


            KStream<String, Message> userIdMessage =
                    builder.stream(String(), mapperSerializer(Message.class), "messages")
                            .map((k, v) -> new KeyValue<>(v.getUserId(), v))
                            .through(String(), mapperSerializer(Message.class), "userIdMessages");

            userIdMessage
                    .countByKey("messageAmount")
                    .toStream()
                    .to(String(), LongStr(), "messageCount");


            userIdMessage
                    .map((k, v) -> new KeyValue<>(v.getText(), v.getRate()))
                    .aggregateByKey(() -> new AverageRate(0, 0), (k, rate, ag) -> ag.add(rate),
                            TumblingWindows.of("averageRate").with(10000), String(), mapperSerializer(AverageRate.class))
                    .toStream()
                    .mapValues(AverageRate::computeRate)
                    .to(Windowed(), DoubleStr(), "averageRateByText");

            userIdMessage
                    .mapValues(Message::getRate)
                    .aggregateByKey(() -> new AverageRate(0, 0), (k, rate, ag) -> ag.add(rate),
                            HoppingWindows.of("averageHoppingRate").with(30000).every(10000),
                            String(), mapperSerializer(AverageRate.class))
                    .toStream()
                    .mapValues(AverageRate::computeRate)
                    .filter((k, v) -> v < 1)
                    .to(Windowed(), DoubleStr(), "lowRatingUsers")
                    ;
        });


    }


}
