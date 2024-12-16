package tp3;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import tp3.models.Route;
import tp3.serdes.JsonSerde;
import tp3.serdes.RouteSerde;

public class TempStream {
    public static void main(String[] args) {

        String inputTopic = "routes-topic";

        String outputTopic = "end";

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-example");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Route> textLines = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), new JsonSerde<>(Route.class)));

        textLines.mapValues(value -> {
            System.out.println("Read from routes-topic: Value = " + value);
            return value.getOperator();
        }).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(
                new Thread("streams-shutdown-hook") {
                    @Override
                    public void run() {
                        streams.close();
                        latch.countDown();
                    }
                });
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
