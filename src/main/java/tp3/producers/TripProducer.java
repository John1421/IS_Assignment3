package tp3.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import lombok.extern.slf4j.Slf4j;
import tp3.models.Trip;
import tp3.serdes.JsonSerde;
import tp3.utils.RouteIdFetcher;

import java.util.List;
import java.util.Properties;
import java.util.Random;

@Slf4j
public class TripProducer {

    private static final String BOOTSTRAP_SERVERS = "broker1:9092";
    private static final String TOPIC = "trips-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaProducer<String, Trip> producer = new KafkaProducer<>(properties,
                new StringSerializer(), new JsonSerde<>(Trip.class));

        // Fetch existing route IDs
        List<Long> existingRouteIds = RouteIdFetcher.fetchRouteIds();
        log.info("Fetched existing route IDs: {}", existingRouteIds);

        if (existingRouteIds.isEmpty()) {
            log.warn("No route IDs found in the topic. Exiting.");
            producer.close();
            return;
        }

        Random random = new Random();

        try {
            for (int i = 0; i < 100; i++) {
                Long routeId = existingRouteIds.get(random.nextInt(existingRouteIds.size())); // Pick a random route ID
                Trip trip = new Trip(); // Create new Trip object
                trip.setRouteId(routeId); // Assign valid routeId

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
