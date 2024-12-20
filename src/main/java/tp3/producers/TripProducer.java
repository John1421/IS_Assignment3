package tp3.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import lombok.extern.slf4j.Slf4j;
import tp3.models.Route;
import tp3.models.Trip;
import tp3.serdes.JsonSerde;
import tp3.utils.RouteIdFetcher;

import java.util.List;
import java.util.Properties;
import java.util.Random;

@Slf4j
public class TripProducer {

    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093,broker3:9094";

    private static final String TOPIC = "trips-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        KafkaProducer<String, Trip> producer = new KafkaProducer<>(properties,
                new StringSerializer(), new JsonSerde<>(Trip.class));

        // Fetch existing route IDs
        List<Route> existingRoutes = RouteIdFetcher.fetchRoutes();
        log.info("Fetched existing route IDs: {}", existingRoutes);

        if (existingRoutes.isEmpty()) {
            log.warn("No route IDs found in the topic. Exiting.");
            producer.close();
            return;
        }

        Random random = new Random();

        try {
            for (int i = 0; i < 100; i++) {
                Route route = existingRoutes.get(random.nextInt(existingRoutes.size())); // Random route

                Trip trip = new Trip(); // Create new Trip object
                trip.setRouteId(route.getId()); // Assign valid routeId
                trip.setOrigin(route.getOrigin());
                trip.setDestination(route.getDestination());
                trip.setTransportType(route.getTransportType());

                String key = "trip_" + trip.getId(); // Create key
                ProducerRecord<String, Trip> record = new ProducerRecord<>(TOPIC, key, trip);

                // Send data asynchronously
                producer.send(record, (RecordMetadata metadata, Exception e) -> {
                    if (e == null) {
                        // Log success
                        log.info("Successfully sent trip: \n" +
                                "Key: " + key + "\n" +
                                "Value: " + trip + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset());
                    } else {
                        log.error("Error while producing", e);
                    }
                });

                Thread.sleep(100); // Simulate delay between messages
            }
        } catch (InterruptedException e) {
            log.error("Error while producing messages", e);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
