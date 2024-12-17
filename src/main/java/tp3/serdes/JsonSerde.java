package tp3.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serde;
import tp3.models.Operator;

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
