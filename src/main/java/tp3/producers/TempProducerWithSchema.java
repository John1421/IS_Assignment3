package tp3.producers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public class TempProducerWithSchema {

    private static final String BOOTSTRAP_SERVERS = "broker1:9092";
    private static final String TOPIC = "req1";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            for (int i = 0; i < 10; i++) {
                String key = "V" + i; // Kafka key
                Map<String, Object> message = Map.of(
                    "schema", Map.of(
                        "type", "struct",
                        "fields", new Object[] {
                            Map.of("type", "string", "optional", false, "field", "operator")
                        },
                        "optional", false,
                        "name", "record"
                    ),
                    "payload", Map.of("operator", "Operator " + i)
                );

                String value = objectMapper.writeValueAsString(message);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Record sent successfully: " + metadata);
                    } else {
                        exception.printStackTrace();
                    }
                });

                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
