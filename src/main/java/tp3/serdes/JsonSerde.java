package tp3.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Class<T> targetType; // Removed final to allow later assignment

    public JsonSerde() {
        //
    }

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    public void setTargetType(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No-op
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
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
