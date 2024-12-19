package tp3.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TransportTypeOccupancy {
    private String transportType;
    private Float occupancyPercentage;
}
