package tp3;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import tp3.models.Operator;
import tp3.models.Route;
import tp3.models.Number;
import tp3.models.RouteOccupancy;
import tp3.models.Trip;
import tp3.serdes.JsonSerde;

public class Streams {

    private static final String ROUTES_TOPIC = "routes-topic";
    private static final String TRIPS_TOPIC = "trips-topic";
    private static final String OPERATORS_FROM_DB = "operators-from-db";
    private static final String PASSENGERS_PER_ROUTE_TOPIC = "passengers-per-route-topic";
    private static final String AVAILABLE_SEATS_PER_ROUTE_TOPIC = "req5";
    private static final String OCCUPANCY_PER_ROUTE_TOPIC = "occupancy-per-route-topic";
    private static final String TOTAL_PASSANGERS_TOPIC = "req7";

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
                (key, trip) -> {
                    if (trip != null) {
                        System.out.println("Trip processing: " + trip.getId()); // TODO: remove logs at the end
                        return trip.getRouteId();
                    }
                    return null;
                }, // Use routeId as the grouping key
                Grouped.with(Serdes.Long(), new JsonSerde<>(Trip.class)) // Specify the serdes for grouping
        );

        // KTable for passengers per route
        KTable<Long, Long> passengersPerRoute = tripsGroupedByRoute
                .count(Materialized.with(Serdes.Long(), Serdes.Long()));
        passengersPerRoute.toStream()
                .mapValues((routeId, count) -> new Number(routeId, count))
                .to(PASSENGERS_PER_ROUTE_TOPIC, Produced.with(Serdes.Long(), new JsonSerde<>(Number.class)));

        // -------------------- REQ 5 --------------------
        // Map routesStream to extract routeId as key and passengerCapacity as value
        KGroupedStream<Long, Long> routesGroupedByRoute = routesStream
                .map((key, route) -> KeyValue.pair(route.getId(), route.getPassengerCapacity())) // Use routeId as key
                                                                                                 // and capacity as
                                                                                                 // value
                .groupByKey(Grouped.with(Serdes.Long(), Serdes.Long())); // Group by routeId

        // Reduce to calculate total capacity per route
        KTable<Long, Long> totalCapacityPerRoute = routesGroupedByRoute.reduce(
                Long::sum, // Sum passenger capacity for the same routeId
                Materialized.with(Serdes.Long(), Serdes.Long()) // Materialize the result
        );

        // Join totalCapacityPerRoute with passengersPerRoute to calculate available
        // seats
        KTable<Long, Long> availableSeatsPerRoute = totalCapacityPerRoute.leftJoin(
                passengersPerRoute,
                (capacity, passengers) -> {
                    if (passengers == null)
                        return capacity; // No passengers recorded
                    return capacity - passengers; // Calculate available seats
                },
                Materialized.with(Serdes.Long(), Serdes.Long()) // Materialize the result
        );

        // Write the result to the output topic
        availableSeatsPerRoute.toStream()
                .map((routeId, seatCount) -> {
                    System.out.println("Route ID: " + routeId + " has available seats: " + seatCount);
                    return KeyValue.pair(routeId, new Number(routeId, seatCount));
                })
                .to(AVAILABLE_SEATS_PER_ROUTE_TOPIC, Produced.with(Serdes.Long(), new JsonSerde<>(Number.class)));

        // -------------------- REQ 6 --------------------
        // Compute occupancy percentage per route
        KTable<Long, Float> occupancyPerRoute = passengersPerRoute.join(
                availableSeatsPerRoute,
                (passengerCount, seatCount) -> {
                    if (seatCount == null || seatCount == 0) {
                        return (float) 0.0; // Avoid division by zero
                    }
                    return ((float) passengerCount / (passengerCount + seatCount)) * 100;
                },
                Materialized.with(Serdes.Long(), Serdes.Float())
        );

        occupancyPerRoute.toStream()
                .map((routeId, occupancyPercentage) -> {
                    System.out.println("Occupancy for route " + routeId + " is " + occupancyPercentage + "%");
                    return KeyValue.pair(routeId, new RouteOccupancy(routeId, occupancyPercentage));
                })
                .to(OCCUPANCY_PER_ROUTE_TOPIC, Produced.with(Serdes.Long(), new JsonSerde<>(RouteOccupancy.class)));

        
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
