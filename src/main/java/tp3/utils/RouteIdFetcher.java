package tp3.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import tp3.models.Route;
import tp3.serdes.JsonSerde;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class RouteIdFetcher {
    private static final String BOOTSTRAP_SERVERS = "broker1:9092";
    private static final String TOPIC = "routes-topic";
    private static final String GROUP_ID = "route-id-fetcher";

    public static List<Long> fetchRouteIds() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonSerde.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<Long> routeIds = new ArrayList<>();
        KafkaConsumer<String, Route> consumer = new KafkaConsumer<>(properties, new org.apache.kafka.common.serialization.StringDeserializer(), new JsonSerde<>(Route.class));

        try {
            consumer.subscribe(List.of(TOPIC));
            ConsumerRecords<String, Route> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, Route> record : records) {
                Route route = record.value();
                if (route != null) {
                    routeIds.add(route.getId());
                }
            }
        } finally {
            consumer.close();
        }

        return routeIds;
    }
}
