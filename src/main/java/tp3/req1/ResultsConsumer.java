package tp3.req1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ResultsConsumer {

    private static final String BOOTSTRAP_SERVERS = "broker1:9092";
    private static final String TOPIC = "req1";
    private static final String GROUP_ID = "req1-consumer-group";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/yourdb";
    private static final String DB_USER = "youruser";
    private static final String DB_PASSWORD = "yourpassword";

    public static void main(String[] args) {
        // Kafka consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {

            consumer.subscribe(Collections.singletonList(TOPIC));

            // Process messages
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // Assuming the message is a JSON string
                    System.out.printf("Consumed record with key %s and value %s%n", record.key(), record.value());

                    // Insert data into the database
                    insertToDatabase(connection, record.key(), record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertToDatabase(Connection connection, String key, String value) {
        String insertSQL = "INSERT INTO results_table (result_key, result_value) VALUES (?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            preparedStatement.setString(1, key);
            preparedStatement.setString(2, value);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
