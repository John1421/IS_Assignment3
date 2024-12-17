package tp3.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
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
            // Check if we need to serialize as a Struct with schema (for JDBC Sink)
            if (data instanceof Operator) {
                Operator operator = (Operator) data;

                // Create the Struct for the Operator class using the pre-defined schema
                Schema schema = OperatorSchema.OPERATOR_SCHEMA;
                Struct struct = new Struct(schema)
                        .put("operator", operator.getOperator()); // Add the operator field to the Struct

                // Serialize the Struct to JSON with schema and payload
                return OBJECT_MAPPER.writeValueAsBytes(struct);
            }
            // Default case: Serialize the data directly if it's not a custom object
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
            // Deserialize JSON into a Struct
            Struct struct = OBJECT_MAPPER.readValue(data, Struct.class);

            // Extract the operator field from the Struct and convert it to the Operator
            // object
            String operator = struct.getString("operator");
            Operator operatorObject = new Operator(operator); // Create Operator object from struct

            return (T) operatorObject;
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
