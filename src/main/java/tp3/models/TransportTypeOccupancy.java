package tp3.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransportTypeOccupancy {
    private String transportType;
    private Float occupancyPercentage;
}
