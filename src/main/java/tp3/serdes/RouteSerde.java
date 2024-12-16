package tp3.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;

import tp3.models.Route;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class RouteSerde implements Serializer<Route>, Deserializer<Route> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public RouteSerde() {
        // No-op constructor
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No-op
    }

    @Override
    public byte[] serialize(String topic, Route route) {
        if (route == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writeValueAsBytes(route);
        } catch (Exception e) {
            throw new SerializationException("Error serializing Route message", e);
        }
    }

    @Override
    public Route deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(data, Route.class);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing Route message", e);
        }
    }

    @Override
    public void close() {
        // No-op
    }

    public static class RouteSerdeSerde extends Serdes.WrapperSerde<Route> {
        public RouteSerdeSerde() {
            super(new RouteSerde(), new RouteSerde());
        }
    }
}
