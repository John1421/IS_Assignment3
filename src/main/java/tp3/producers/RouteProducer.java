package tp3.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.json.JsonConverter;

import lombok.extern.slf4j.Slf4j;
import tp3.models.Route;

import java.util.Properties;

@Slf4j
public class RouteProducer {

    private static final String BOOTSTRAP_SERVERS = "broker1:9092";
    private static final String TOPIC = "routes-topic";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonConverter.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            for (int i = 0; i < 10; i++) {
                Route route = new Route(); // Create new Trip object
                String key = "route_" + route.getId(); // Create key

                String value = objectMapper.writeValueAsString(route); // Serialize Trip object to JSON

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                // Send data asynchronously
                producer.send(record, (RecordMetadata metadata, Exception e) -> {
                    if (e == null) {
                        // Log success
                        log.info("Successfully sent trip: \n" +
                                "Key: " + key + "\n" +
                                "Value: " + value + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset());
                    } else {
                        log.error("Error while producing", e);
                    }
                });

                Thread.sleep(1000); // TODO See if needed
            }
        } catch (JsonProcessingException | InterruptedException e) {
            log.error("Error while producing messages", e);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}