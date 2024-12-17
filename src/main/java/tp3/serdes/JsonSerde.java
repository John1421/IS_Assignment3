package tp3.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serde;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Map;

public class JsonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Class<T> targetType; // Target type for deserialization
    private boolean isKey; // Flag to check if the serde is for key or value

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
            // Serialize both schema and payload if it's a record with a schema
            if (data instanceof Struct) {
                Struct struct = (Struct) data;
                ObjectNode message = OBJECT_MAPPER.createObjectNode();

                // Schema
                Schema schema = struct.schema();
                ObjectNode schemaNode = OBJECT_MAPPER.createObjectNode();
                schemaNode.put("type", schema.type().toString());
                schemaNode.put("name", schema.name());

                // Fields (assuming the schema contains a list of fields)
                schema.fields().forEach(field -> {
                    ObjectNode fieldNode = OBJECT_MAPPER.createObjectNode();
                    fieldNode.put("name", field.name());
                    fieldNode.put("type", field.schema().type().toString());
                    schemaNode.set(field.name(), fieldNode);
                });

                // Payload
                ObjectNode payloadNode = OBJECT_MAPPER.createObjectNode();
                schema.fields().forEach(field -> {
                    payloadNode.put(field.name(), struct.get(field).toString());
                });

                message.set("schema", schemaNode);
                message.set("payload", payloadNode);

                return OBJECT_MAPPER.writeValueAsBytes(message);
            }
            return OBJECT_MAPPER.writeValueAsBytes(data); // Default serialization
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
            // Deserialize the JSON into a Struct (schema-aware)
            ObjectNode jsonNode = (ObjectNode) OBJECT_MAPPER.readTree(data);
            ObjectNode schemaNode = (ObjectNode) jsonNode.get("schema");
            ObjectNode payloadNode = (ObjectNode) jsonNode.get("payload");

            // We need to handle the schema and payload appropriately.
            // You can define a method to map schema fields and convert payload to Struct
            if (schemaNode != null && payloadNode != null) {
                Schema schema = parseSchema(schemaNode); // Parse the schema
                Struct struct = new Struct(schema);

                // Map the payload to the Struct
                schema.fields().forEach(field -> {
                    struct.put(field, payloadNode.get(field.name()).asText());
                });

                return (T) struct;
            } else {
                // Handle the case where schema is not available
                return OBJECT_MAPPER.readValue(data, targetType);
            }

        } catch (IOException e) {
            throw new SerializationException("Error deserializing JSON message", e);
        }
    }

    private Schema parseSchema(ObjectNode schemaNode) {
        // Custom logic to parse the schema node into a Schema
        // This will depend on how your schema is structured in the JSON
        // For now, let's assume we just return a simple schema for illustration:
        return Schema.STRING_SCHEMA; // Placeholder, implement actual logic here
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
