package tp3;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import tp3.models.Route;
import tp3.serdes.RouteSerde;

public class StreamsOld {
    private static final String routesTopic = "routes-topic";
    // private static final String tripsTopic = "trips-topic";
    // private static final String operatorsTopic = "operatorsFromDatabase";
    private static final String resultsTopic = "results-topic";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "project3");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RouteSerde.class);

        StreamsBuilder builder = new StreamsBuilder();

        // -------------------- REQ 1 --------------------
        // Stream from routes-topic to extract and deduplicate suppliers
        KStream<String, Route> routesStream = builder.stream(routesTopic);

        KTable<String, String> uniqueOperators = routesStream
                .mapValues(value -> {
                    System.out.println("Read from routes-topic: Value = " + value);
                    return value.getOperator();
                })
                .filter((key, operator) -> operator != null) // Remove null values
                .groupBy((key, operator) -> operator) // Group by operator
                .reduce((aggValue, newValue) -> aggValue); // Remove duplicate operators

        uniqueOperators.toStream().to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));

        // // -------------------- REQ 2 --------------------
        // // Stream to list route operators
        // KStream<String, String> operatorsStream = builder.stream(operatorsTopic);

        // operatorsStream.foreach((key, value) -> {
        // try {
        // JsonNode jsonNode = objectMapper.readTree(value);
        // String supplierName = jsonNode.get("payload").get("name").asText();
        // System.out.println("Supplier: " + supplierName);
        // } catch (Exception e) {
        // System.err.println("Failed to parse message: " + value);
        // e.printStackTrace();
        // }
        // });

        // Start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(
                new Thread() {
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