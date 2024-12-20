package tp3;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import tp3.models.Operator;
import tp3.models.Route;
import tp3.models.NameNumber;
import tp3.models.Number;
import tp3.models.RouteOccupancy;
import tp3.models.RouteOccupancyWithAddedField;
import tp3.models.TransporType;
import tp3.models.TransportTypeOccupancy;
import tp3.models.Trip;
import tp3.serdes.JsonSerde;

public class Streams {

        private static final String ROUTES_TOPIC = "routes-topic";
        private static final String TRIPS_TOPIC = "trips-topic";
        private static final String OPERATORS_FROM_DB = "operators-from-db";
        private static final String OPERATORS_TOPIC = "req1-topic";
        private static final String PASSENGERS_PER_ROUTE_TOPIC = "req4-topic";
        private static final String AVAILABLE_SEATS_PER_ROUTE_TOPIC = "req5-topic";
        private static final String OCCUPANCY_PER_ROUTE_TOPIC = "req6-topic";
        private static final String TOTAL_PASSANGERS_TOPIC = "req7-topic";
        private static final String TOTAL_SEATING_CAPACITY_TOPIC = "req8-topic";
        private static final String OCCUPANCY_PERCENTAGE_TOTAL_TOPIC = "req9-topic";
        private static final String AVERAGE_PASSENGERS_PER_TRANSPORT_TYPE = "req10-topic";
        private static final String TRANSPORT_TYPE_WITH_MOST_PASSANGERS = "req11-topic";
        private static final String ROUTE_WITH_LEAST_OCCUPANCY_TOPIC = "req12-topic";
        private static final String MOST_USED_TRANSPORT_TYPE_LAST_HOUR = "req13-topic";
        private static final String LEAST_OCUPIED_TRANSPORT_TYPE = "req14-topic";
        private static final String OPERATOR_WITH_MOST_OCCUPANCY_TOPIC = "req15-topic";
        private static final String PASSENGER_WITH_MOST_TRIPS = "req16-topic";

        public static void main(String[] args) {

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
                }).to(OPERATORS_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Operator.class)));

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
                                                System.out.println("Trip processing: " + trip.getId()); // TODO: remove
                                                                                                        // logs at the
                                                                                                        // end
                                                return trip.getRouteId();
                                        }
                                        return null;
                                }, // Use routeId as the grouping key
                                Grouped.with(Serdes.Long(), new JsonSerde<>(Trip.class)) // Specify the serdes for
                                                                                         // grouping
                );

                // KTable for passengers per route
                KTable<Long, Long> passengersPerRoute = tripsGroupedByRoute
                                .count(Materialized.with(Serdes.Long(), Serdes.Long()));
                passengersPerRoute.toStream()
                                .mapValues((routeId, count) -> new Number(routeId, count))
                                .to(PASSENGERS_PER_ROUTE_TOPIC,
                                                Produced.with(Serdes.Long(), new JsonSerde<>(Number.class)));

                // -------------------- REQ 5 --------------------
                // Map routesStream to extract routeId as key and passengerCapacity as value
                KGroupedStream<Long, Long> routesGroupedByRoute = routesStream
                                .map((key, route) -> KeyValue.pair(route.getId(), route.getPassengerCapacity()))
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
                                        System.out.println(
                                                        "Route ID: " + routeId + " has available seats: " + seatCount);
                                        return KeyValue.pair(routeId, new Number(routeId, seatCount));
                                })
                                .to(AVAILABLE_SEATS_PER_ROUTE_TOPIC,
                                                Produced.with(Serdes.Long(), new JsonSerde<>(Number.class)));

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
                                Materialized.with(Serdes.Long(), Serdes.Float()));

                occupancyPerRoute.toStream()
                                .map((routeId, occupancyPercentage) -> {
                                        System.out.println("Occupancy for route " + routeId + " is "
                                                        + occupancyPercentage + "%");
                                        return KeyValue.pair(routeId, new RouteOccupancy(routeId, occupancyPercentage));
                                })
                                .to(OCCUPANCY_PER_ROUTE_TOPIC,
                                                Produced.with(Serdes.Long(), new JsonSerde<>(RouteOccupancy.class)));

                // -------------------- REQ 7 --------------------
                // Calculate total passengers from the passengersPerRoute table
                KTable<String, Number> totalPassengers = passengersPerRoute
                                .toStream()
                                .map((routeId, count) -> KeyValue.pair("total", count)) // Use a constant key "total"
                                                                                        // for all records
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())) // Group all records by the
                                                                                          // constant key
                                .reduce(
                                                Long::sum, // Sum passenger counts for the constant key
                                                Materialized.with(Serdes.String(), Serdes.Long()) // Materialize the
                                                                                                  // result
                                ).mapValues((value) -> {
                                        return new Number(0, value);
                                });
                // Write the total passenger count to a new topic
                totalPassengers.toStream().to(TOTAL_PASSANGERS_TOPIC,
                                Produced.with(Serdes.String(), new JsonSerde<>(Number.class)));

                // -------------------- REQ 8 --------------------
                // Calculate total seating available for all routes
                KTable<String, Number> totalSeatingAvailable = availableSeatsPerRoute
                                .toStream()
                                .map((routeId, availableSeats) -> KeyValue.pair("total", availableSeats))
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                                .reduce(
                                                Long::sum,
                                                Materialized.with(Serdes.String(), Serdes.Long()))
                                .mapValues(totalAvailableSeats -> new Number(0, totalAvailableSeats));

                totalSeatingAvailable.toStream().to(TOTAL_SEATING_CAPACITY_TOPIC,
                                Produced.with(Serdes.String(), new JsonSerde<>(Number.class)));

                // -------------------- REQ 9 --------------------
                // Calculate total occupancy percentage for all routes

                KTable<String, Float> totalOccupancyPercentage = totalPassengers.join(
                                totalSeatingAvailable,
                                (passengers, seating) -> {
                                        if (seating.getValue() == 0) { // Avoid division by zero
                                                return 0.0f;
                                        }
                                        return ((float) passengers.getValue()
                                                        / (passengers.getValue() + seating.getValue())) * 100;
                                },
                                Materialized.with(Serdes.String(), Serdes.Float()) // Materialize as Float
                );

                totalOccupancyPercentage.toStream()
                                .mapValues(occupancy -> {
                                        System.out.println("Total occupancy percentage for all routes: " + occupancy
                                                        + "%");
                                        return new RouteOccupancy(0l, occupancy);
                                })
                                .to(OCCUPANCY_PERCENTAGE_TOTAL_TOPIC,
                                                Produced.with(Serdes.String(), new JsonSerde<>(RouteOccupancy.class)));

                // -------------------- REQ 10 --------------------
                // Calculate the average number of passengers per transport type

                // Group routes by transport type
                KGroupedStream<String, Route> routesGroupedByTransportType = routesStream.groupBy(
                                (key, route) -> {
                                        if (route != null) {
                                                System.out.println("Grouping routes by transport type: "
                                                                + route.getTransportType());
                                                return route.getTransportType(); // Group by transport type
                                        }
                                        return null;
                                },
                                Grouped.with(Serdes.String(), new JsonSerde<>(Route.class)));

                // Count distinct routes per transport type
                KTable<String, Long> totalRoutesPerTransportType = routesGroupedByTransportType.count(
                                Materialized.with(Serdes.String(), Serdes.Long()) // Count distinct routes
                );

                // Group trips by transport type
                KGroupedStream<String, Trip> tripsGroupedByTransportType = tripsStream.groupBy(
                                (key, trip) -> {
                                        if (trip != null) {
                                                return trip.getTransportType(); // Group by transport type
                                        }
                                        return null;
                                },
                                Grouped.with(Serdes.String(), new JsonSerde<>(Trip.class)));

                // Calculate total passengers per transport type
                KTable<String, Long> totalPassengersPerTransportType = tripsGroupedByTransportType.aggregate(
                                () -> 0L,
                                (key, trip, aggregate) -> {
                                        System.out.println("Adding passenger for transport type: " + key);
                                        return aggregate + 1; // Increment for each passenger
                                },
                                Materialized.with(Serdes.String(), Serdes.Long()));

                // Join totalPassengersPerTransportType with totalRoutesPerTransportType
                KTable<String, Double> averagePassengersPerTransportType = totalPassengersPerTransportType.join(
                                totalRoutesPerTransportType,
                                (totalPassengerCount, totalRouteCount) -> {
                                        if (totalRouteCount == 0) {
                                                System.out.println(
                                                                "No routes for transport type, setting average to 0.0");
                                                return 0.0; // Avoid division by zero
                                        }
                                        System.out.println(
                                                        "Calculating average for transport type: totalPassengerCount="
                                                                        + totalPassengerCount + ", totalRouteCount="
                                                                        + totalRouteCount);
                                        return (double) totalPassengerCount / totalRouteCount; // Calculate the average
                                },
                                Materialized.with(Serdes.String(), Serdes.Double()) // Materialize the result
                );

                // Write the result to a new topic
                averagePassengersPerTransportType.toStream()
                                .map((transportType, average) -> {
                                        System.out.println("Average passengers for " + transportType + ": " + average);
                                        return KeyValue.pair(transportType,
                                                        new NameNumber(transportType, average.longValue()));
                                })
                                .to(AVERAGE_PASSENGERS_PER_TRANSPORT_TYPE,
                                                Produced.with(Serdes.String(), new JsonSerde<>(NameNumber.class)));

                // -------------------- REQ 11 --------------------
                // Get the transport type with the highest number of served passengers

                // Count passengers per transport type
                KTable<String, Long> passengersPerTransportType = tripsGroupedByTransportType.count(
                                Materialized.with(Serdes.String(), Serdes.Long()));

                // Find the maximum transport type
                KTable<String, Long> maxTransportType = passengersPerTransportType
                                .toStream()
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                                .reduce((currentValue, newValue) -> {
                                        return Math.max(currentValue, newValue);
                                });

                // Output the result
                maxTransportType.toStream()
                                .map((key, value) -> {
                                        System.out.println("Transport type with the highest passengers: " + key);
                                        return KeyValue.pair("highest", new TransporType("highest", key));
                                })
                                .to(TRANSPORT_TYPE_WITH_MOST_PASSANGERS,
                                                Produced.with(Serdes.String(), new JsonSerde<>(TransporType.class)));

                // -------------------- REQ 12 --------------------
                // Get the routes with the least occupancy per transport type

                // Step 1: Convert routesStream to KTable for efficient lookup by routeId
                KTable<Long, Route> routesTable = routesStream
                                .selectKey((key, route) -> route.getId()) // Re-key routes by routeId
                                .toTable(Materialized.with(Serdes.Long(), new JsonSerde<>(Route.class)));

                // Step 2: Enrich occupancyPerRoute with transport type using a left join
                KTable<Long, RouteOccupancyWithAddedField> occupancyWithTransportType = occupancyPerRoute.join(
                                routesTable,
                                (occupancy, route) -> {
                                        if (route == null) {
                                                return new RouteOccupancyWithAddedField(null, null, occupancy);
                                        }
                                        return new RouteOccupancyWithAddedField(route.getId(),
                                                        route.getTransportType(), occupancy);
                                },
                                Materialized.with(Serdes.Long(),
                                                new JsonSerde<>(RouteOccupancyWithAddedField.class)));

                // Step 3: Group by transport type and find the route with the least occupancy
                KTable<String, RouteOccupancyWithAddedField> leastOccupancyPerTransportType = occupancyWithTransportType
                                .toStream()
                                .groupBy(
                                                (KeyValueMapper<Long, RouteOccupancyWithAddedField, String>) (
                                                                routeId,
                                                                routeOccupancy) -> routeOccupancy.getName(),
                                                Grouped.with(Serdes.String(),
                                                                new JsonSerde<>(RouteOccupancyWithAddedField.class)))
                                .reduce(
                                                (currentOccupancy, newOccupancy) -> {
                                                        if (currentOccupancy.getOccupancyPercentage() <= newOccupancy
                                                                        .getOccupancyPercentage()) {
                                                                return currentOccupancy;
                                                        } else {
                                                                return newOccupancy;
                                                        }
                                                },
                                                Materialized.with(Serdes.String(), new JsonSerde<>(
                                                                RouteOccupancyWithAddedField.class)));

                // Step 4: Output the results to a new topic
                leastOccupancyPerTransportType.toStream()
                                .map((transportType, routeOccupancy) -> {
                                        System.out.println("Least occupied route for transport type " + transportType
                                                        + ": "
                                                        + "RouteId=" + routeOccupancy.getRouteId() + ", Occupancy="
                                                        + routeOccupancy.getOccupancyPercentage());
                                        return KeyValue.pair(transportType, routeOccupancy);
                                })
                                .to(ROUTE_WITH_LEAST_OCCUPANCY_TOPIC,
                                                Produced.with(Serdes.String(), new JsonSerde<>(
                                                                RouteOccupancyWithAddedField.class)));

                // -------------------- REQ 13 --------------------
                // Get the most used transport type in the last hour using a tumbling time
                // window
                TimeWindows hourWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1));
                // Apply a tumbling window of 1 hour

                KStream<String, Long> transportTypeHourCount = routesGroupedByTransportType
                                .windowedBy(hourWindow)
                                .count().toStream().map((windowedKey, count) -> {
                                        String transportType = windowedKey.key(); // Extract transport type from the
                                        // windowed key
                                        System.out.println("Transport type: " + transportType + " had " + count
                                                        + " occurrences in the last hour.");
                                        return KeyValue.pair(transportType, count);
                                });

                // Map to a new stream with a fixed key and NameNumber as the value
                KStream<String, NameNumber> mappedTransportTypeStream = transportTypeHourCount
                                .map((transportType, count) -> KeyValue.pair("most_used",
                                                new NameNumber(transportType, count)));

                // Group the mapped stream by the fixed key ("most_used")
                KGroupedStream<String, NameNumber> groupedStream = mappedTransportTypeStream.groupByKey(
                                Grouped.with(Serdes.String(), new JsonSerde<>(NameNumber.class)));

                // Reduce to find the transport type with the highest count
                KTable<String, NameNumber> mostUsedTransportTypeLastHour = groupedStream.reduce(
                                (current, next) -> {
                                        if (current.getValue() > next.getValue()) {
                                                return current;
                                        }
                                        return next;
                                },
                                Materialized.with(Serdes.String(), new JsonSerde<>(NameNumber.class)));

                // Write the result to the output topic
                mostUsedTransportTypeLastHour.toStream()
                                .mapValues((key, value) -> {
                                        System.out.println("Most used transport type in the last hour: "
                                                        + value.getId());
                                        return value;
                                })
                                .to(MOST_USED_TRANSPORT_TYPE_LAST_HOUR,
                                                Produced.with(Serdes.String(), new JsonSerde<>(NameNumber.class)));

                // -------------------- REQ 14 --------------------
                // Map occupancyPerRoute to include transport type by joining with routesStream
                KTable<Long, Route> routeWithTransportStream = routesStream
                                .map((key, route) -> KeyValue.pair(route.getId(), route)).toTable(); // Map routeId to
                                                                                                     // route object

                KTable<String, Float> occupancyPerRouteWithTransport = occupancyPerRoute
                                .toStream()
                                .join(
                                                routeWithTransportStream,
                                                (occupancy, route) -> new TransportTypeOccupancy(
                                                                route.getTransportType(), occupancy), // Map
                                                                                                      // routeId
                                                                                                      // to
                                                // transportType
                                                Joined.with(Serdes.Long(), Serdes.Float(),
                                                                new JsonSerde<>(Route.class)))
                                .groupBy(
                                                (routeId, transportAndOccupancy) -> transportAndOccupancy
                                                                .getTransportType())
                                .aggregate(
                                                // Initializer: Start with (totalOccupancy = 0, count = 0)
                                                () -> new double[] { 0.0, 0.0 },
                                                // Aggregator: Update total occupancy and count
                                                (key, newValue, aggregate) -> {
                                                        aggregate[0] += newValue.getOccupancyPercentage(); // Update
                                                                                                           // total
                                                                                                           // occupancy
                                                        aggregate[1] += 1; // Increment count
                                                        return aggregate;
                                                },
                                                Materialized.with(Serdes.String(), new JsonSerde<>(double[].class)))
                                // Transform to compute the average
                                .mapValues(aggregate -> (float) (aggregate[0] / aggregate[1]));

                // Find the transport type with the minimum occupancy
                KTable<String, TransporType> minOccupancyTransportType = occupancyPerRouteWithTransport
                                .toStream()
                                .map(
                                                (transportType, averageOccupancy) -> KeyValue.pair(
                                                                "min_occupancy", // Use a fixed key for aggregation
                                                                new TransportTypeOccupancy(transportType,
                                                                                averageOccupancy) // Create the object
                                                )) // Serializer
                                .groupByKey()
                                .reduce(
                                                (current, next) -> current.getOccupancyPercentage() < next
                                                                .getOccupancyPercentage()
                                                                                ? current
                                                                                : next)
                                .mapValues((v) -> {
                                        return new TransporType("min_occupancy", v.getTransportType());
                                });

                minOccupancyTransportType.toStream().to(LEAST_OCUPIED_TRANSPORT_TYPE,
                                Produced.with(Serdes.String(), new JsonSerde<>(TransporType.class)));

                // -------------------- REQ 15 --------------------
                // Get the name of the route operator with the most occupancy, including the
                // occupancy value

                // Step 1: Enrich occupancyPerRoute with operator information using a join
                KTable<Long, RouteOccupancyWithAddedField> enrichedOccupancyWithOperator = occupancyPerRoute.join(
                                routesTable,
                                (occupancy, route) -> {
                                        if (route == null) {
                                                return new RouteOccupancyWithAddedField(null, null, occupancy);
                                        }
                                        return new RouteOccupancyWithAddedField(route.getId(), route.getOperator(),
                                                        occupancy);
                                },
                                Materialized.with(Serdes.Long(),
                                                new JsonSerde<>(RouteOccupancyWithAddedField.class)));

                // Step 2: Find the route with the highest occupancy
                KTable<String, RouteOccupancyWithAddedField> maxOccupancyPerOperator = enrichedOccupancyWithOperator
                                .toStream()
                                .groupBy(
                                                (routeId, occupancyWithOperator) -> occupancyWithOperator
                                                                .getName(),
                                                Grouped.with(Serdes.String(),
                                                                new JsonSerde<>(RouteOccupancyWithAddedField.class)))
                                .reduce(
                                                (currentOccupancy, newOccupancy) -> {
                                                        if (currentOccupancy.getOccupancyPercentage() >= newOccupancy
                                                                        .getOccupancyPercentage()) {
                                                                return currentOccupancy;
                                                        } else {
                                                                return newOccupancy;
                                                        }
                                                },
                                                Materialized.with(Serdes.String(), new JsonSerde<>(
                                                                RouteOccupancyWithAddedField.class)));

                // Step 3: Reduce globally to find the operator with the highest occupancy
                KTable<String, RouteOccupancyWithAddedField> globalMaxOccupancy = maxOccupancyPerOperator
                                .toStream()
                                .map((operator, occupancy) -> KeyValue.pair("global_max", occupancy))
                                .groupByKey(Grouped.with(Serdes.String(),
                                                new JsonSerde<>(RouteOccupancyWithAddedField.class)))
                                .reduce(
                                                (currentMax, newMax) -> {
                                                        if (currentMax.getOccupancyPercentage() >= newMax
                                                                        .getOccupancyPercentage()) {
                                                                return currentMax;
                                                        } else {
                                                                return newMax;
                                                        }
                                                },
                                                Materialized.with(Serdes.String(),
                                                                new JsonSerde<>(RouteOccupancyWithAddedField.class)));

                // Step 4: Output the results to a new topic
                globalMaxOccupancy.toStream()
                                .map((key, occupancy) -> {
                                        System.out.println("Operator with the most occupancy globally: "
                                                        + occupancy.getName()
                                                        + " (RouteId=" + occupancy.getRouteId()
                                                        + ", Occupancy=" + occupancy.getOccupancyPercentage() + "%)");
                                        return KeyValue.pair(occupancy.getName(), occupancy);
                                })
                                .to(OPERATOR_WITH_MOST_OCCUPANCY_TOPIC, Produced.with(Serdes.String(),
                                                new JsonSerde<>(RouteOccupancyWithAddedField.class)));

                // -------------------- REQ 16 --------------------
                // Get the name of the passenger with the most trips

                // Step 1: Group trips by passengerId and count the number of trips per
                // passenger
                KTable<Long, Long> tripsCountPerPassenger = tripsStream
                                .groupBy(
                                                (key, trip) -> trip.getPassengerId(), // Group by passengerId
                                                Grouped.with(Serdes.Long(), new JsonSerde<>(Trip.class))
                                )
                                .count(Materialized.with(Serdes.Long(), Serdes.Long()));

                // Step 2: Reduce globally to find the passenger with the most trips
                KTable<String, NameNumber> passengerWithMostTrips = tripsCountPerPassenger
                                .toStream()
                                .map((passengerId, tripCount) -> {
                                        // Create a single key for global reduction
                                        return KeyValue.pair("max_trips",
                                                        new NameNumber(passengerId.toString(), tripCount));
                                })
                                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(NameNumber.class)))
                                .reduce(
                                                (currentMax, newMax) -> {
                                                        // Retain the passenger with the highest trip count
                                                        if (currentMax.getValue() >= newMax.getValue()) {
                                                                return currentMax;
                                                        } else {
                                                                return newMax;
                                                        }
                                                },
                                                Materialized.with(Serdes.String(), new JsonSerde<>(NameNumber.class)));

                // Step 3: Output the results to a new topic
                passengerWithMostTrips.toStream()
                                .mapValues((key, value) -> {
                                        System.out.println("Passenger with the most trips: " + value.getId()
                                                        + " with " + value.getValue() + " trips.");
                                        return value;
                                })
                                .to(PASSENGER_WITH_MOST_TRIPS, Produced.with(Serdes.String(), new JsonSerde<>(NameNumber.class)));

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
