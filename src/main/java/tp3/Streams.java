package tp3;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import tp3.models.Operator;
import tp3.models.Route;
import tp3.models.RouteNumber;
import tp3.models.Trip;
import tp3.serdes.JsonSerde;

public class Streams {

    private static final String ROUTES_TOPIC = "routes-topic";
    private static final String TRIPS_TOPIC = "trips-topic";
    private static final String OPERATORS_FROM_DB = "operators-from-db";
    private static final String PASSENGERS_PER_ROUTE_TOPIC = "passengers-per-route-topic";

    public static void main(String[] args) {

        String outputTopic = "req1";

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "project3");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // -------------------- REQ 1 --------------------
        // Add route suppliers to the database

        KStream<String, Route> routesStream = builder.stream(ROUTES_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(Route.class)));

        routesStream.mapValues(value -> {
            System.out.println("Read from routes-topic: Value = " + value);
            return new Operator(value.getOperator());
        }).to(outputTopic, Produced.with(Serdes.String(), new JsonSerde<>(Operator.class)));

        // -------------------- REQ 2 --------------------
        // Stream to list route operators
        KStream<String, Operator> operatorsStream = builder.stream(OPERATORS_FROM_DB,
                Consumed.with(Serdes.String(), new JsonSerde<>(Operator.class)));

        operatorsStream.foreach((key, value) -> {
            System.out.println("Operator: " + value.getOperator());
        });

        // -------------------- REQ 4 --------------------
        // Stream from trips-topic to calculate passengers per route
        KStream<String, Trip> tripsStream = builder.stream(TRIPS_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(Trip.class)));

        // Group by routeId and count passengers
        KGroupedStream<Long, Trip> tripsGroupedByRoute = tripsStream.groupBy(
                (key, trip) -> trip.getRouteId() // Use routeId as the grouping key
        );

        KTable<Long, Long> passengersPerRoute = tripsGroupedByRoute.count();
        // Convert the output to JSON structure and write to "req4-passengers-route"
        passengersPerRoute.toStream()
                .mapValues((routeId, count) -> {
                    // Create a JSON-compatible Map structure
                    return new RouteNumber(routeId, count);
                })
                .to(PASSENGERS_PER_ROUTE_TOPIC,
                        Produced.with(Serdes.Long(), new JsonSerde<>(RouteNumber.class)));

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
