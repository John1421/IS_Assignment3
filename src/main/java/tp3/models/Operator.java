package tp3.models;

import org.apache.kafka.connect.data.Schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Operator {
    private String operator;
    public static final Schema OPERATOR_SCHEMA = Schema
            .builder()
            .field("operator", Schema.STRING_SCHEMA) // Field for the `operator` attribute
            .build();
}
