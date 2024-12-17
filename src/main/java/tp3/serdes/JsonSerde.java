package tp3.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serde;
import tp3.models.Operator;
import tp3.models.Route;

import java.io.IOException;
import java.util.Map;

public class JsonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Class<T> targetType;
    private boolean isKey;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        try {
            // If the data is an instance of Operator, serialize it using its schema
            if (targetType == Operator.class) {
                Operator operator = (Operator) data;

                // Manually create the schema and payload
                Map<String, Object> operatorWithSchema = Map.of(
                        "schema", Map.of(
                                "type", "struct",
                                "fields", new Object[] {
                                        Map.of("type", "string", "optional", false, "field", "operator")
                                },
                                "optional", false,
                                "name", "record"),
                        "payload", Map.of("operator", operator.getOperator())); // Note: Nested under "payload"

                return OBJECT_MAPPER.writeValueAsBytes(operatorWithSchema);
            }
            if (targetType == Route.class) {
                Route route = (Route) data;

                // Manually create the schema and payload for the Route class
                Map<String, Object> routeWithSchema = Map.of(
                        "schema", Map.of(
                                "type", "struct",
                                "fields", new Object[] {
                                        Map.of("type", "int64", "optional", false, "field", "id"),
                                        Map.of("type", "int32", "optional", false, "field", "passengerCapacity"),
                                        Map.of("type", "string", "optional", false, "field", "origin"),
                                        Map.of("type", "string", "optional", false, "field", "destination"),
                                        Map.of("type", "string", "optional", false, "field", "transportType"),
                                        Map.of("type", "string", "optional", false, "field", "operator")
                                },
                                "optional", false,
                                "name", "route_record"),
                        "payload", Map.of(
                                "id", route.getId(),
                                "passengerCapacity", route.getPassengerCapacity(),
                                "origin", route.getOrigin(),
                                "destination", route.getDestination(),
                                "transportType", route.getTransportType(),
                                "operator", route.getOperator()));

                return OBJECT_MAPPER.writeValueAsBytes(routeWithSchema);
            }

            // Default serialization for non-Operator objects
            return OBJECT_MAPPER.writeValueAsBytes(data);

        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        if (targetType == null) {
            throw new SerializationException("Target type is not set for deserialization.");
        }

        try {
            // Deserialize JSON into a Map first
            Map<String, Object> map = OBJECT_MAPPER.readValue(data, Map.class);

            if (targetType == Operator.class) {
                // Extract the "payload" field which contains the actual operator value
                Map<String, Object> payload = (Map<String, Object>) map.get("payload");
                String operator = (String) payload.get("operator");

                // Return an Operator object created from the deserialized value
                Operator operatorObject = new Operator(operator);
                return (T) operatorObject;
            }
            if (targetType == Route.class) {
                // Extract the "payload" field which contains the actual Route values
                Map<String, Object> payload = (Map<String, Object>) map.get("payload");

                // Extract individual fields from the payload
                long id = ((Number) payload.get("id")).longValue();
                int passengerCapacity = ((Number) payload.get("passengerCapacity")).intValue();
                String origin = (String) payload.get("origin");
                String destination = (String) payload.get("destination");
                String transportType = (String) payload.get("transportType");
                String operator = (String) payload.get("operator");

                // Return a Route object created from the deserialized values
                Route routeObject = new Route(id, passengerCapacity, origin, destination, transportType, operator);
                return (T) routeObject;
            }

            // Default deserialization for non-Operator objects
            return OBJECT_MAPPER.readValue(data, targetType);

        } catch (IOException e) {
            throw new SerializationException("Error deserializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

}
