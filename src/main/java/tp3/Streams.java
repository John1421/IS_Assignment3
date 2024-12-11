package tp3;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class Streams {

    public static void main(String[] args) {

        String routesTopic = "routes-topic";
        String tripsTopic = "trips-topic";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "project3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // -------------------- REQ 1 --------------------
        KStream<String, String> req1Stream = builder.stream(routesTopic);

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
