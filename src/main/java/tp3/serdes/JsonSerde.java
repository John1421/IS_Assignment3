package tp3.serdes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

public class JsonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No-op
    }

    // Serialization: Add schema dynamically to payload
    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            Map<String, Object> schemaWithPayload = Map.of(
                    "schema", generateSchema(targetType),
                    "payload", OBJECT_MAPPER.convertValue(data, Map.class));
            return OBJECT_MAPPER.writeValueAsBytes(schemaWithPayload);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    // Deserialization: Extract payload and convert to targetType
    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            Map<String, Object> jsonMap = OBJECT_MAPPER.readValue(data, Map.class);
            Map<String, Object> payload = (Map<String, Object>) jsonMap.get("payload");
            return OBJECT_MAPPER.convertValue(payload, targetType);
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

    // Dynamically generate schema from the target class
    private Map<String, Object> generateSchema(Class<?> clazz) {
        List<Map<String, Object>> fields = new ArrayList<>();
        for (Field field : clazz.getDeclaredFields()) {
            Map<String, Object> fieldSchema = new HashMap<>();
            fieldSchema.put("type", getFieldType(field.getType()));
            fieldSchema.put("optional", false);
            fieldSchema.put("field", field.getName());
            fields.add(fieldSchema);
        }

        return Map.of(
                "type", "struct",
                "fields", fields,
                "optional", false,
                "name", clazz.getSimpleName() + "_record");
    }

    // Map Java types to schema types
    private String getFieldType(Class<?> type) {
        if (type == String.class) {
            return "string";
        } else if (type == int.class || type == Integer.class) {
            return "int32";
        } else if (type == long.class || type == Long.class) {
            return "int64";
        } else if (type == float.class || type == Float.class) {
            return "float";
        } else if (type == double.class || type == Double.class) {
            return "double";
        } else if (type == boolean.class || type == Boolean.class) {
            return "boolean";
        } else {
            return "string"; // Default to string for complex types
        }
    }
}
