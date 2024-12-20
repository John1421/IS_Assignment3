package tp3.producers.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import lombok.extern.slf4j.Slf4j;
import tp3.models.Route;
import tp3.serdes.JsonSerde;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class TestRouteProducer {
    private static final String BOOTSTRAP_SERVERS = "broker1:9092";
    private static final String TOPIC = "routes-topic";
    private static final List<Route> testRoutes = new ArrayList<>();

    public static void main(String[] args) {
        testRoutes.add(new Route(1l, 50l, "Coimbra", "Leiria", "Bus", "FlixBus"));
        testRoutes.add(new Route(2l, 200l, "Lisboa", "Porto", "Plane", "TAP"));
        testRoutes.add(new Route(3l, 5l, "Sintra", "Cascais", "Taxi", "Bolt"));
        testRoutes.add(new Route(4l, 30l, "Viseu", "Aveiro", "Bus", "Rede Expresso"));
        testRoutes.add(new Route(5l, 250l, "Faro", "Guimaraes", "Train", "CP"));
        testRoutes.add(new Route(6l, 10l, "Evora", "Braga", "Taxi", "Uber"));

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaProducer<String, Route> producer = new KafkaProducer<>(properties,
                new StringSerializer(), new JsonSerde<>(Route.class));

        try {
            for (int i = 0; i < 10; i++) {
                Route route = new Route(); // Create new Route object
                String key = "route_" + route.getId(); // Create key

                ProducerRecord<String, Route> record = new ProducerRecord<>(TOPIC, key, route);

                // Send data asynchronously
                producer.send(record, (RecordMetadata metadata, Exception e) -> {
                    if (e == null) {
                        // Log success
                        log.info("Successfully sent route: \n" +
                                "Key: " + key + "\n" +
                                "Value: " + route + "\n" +
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
