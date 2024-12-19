package tp3.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RouteOccupancyWithTransportType {
    private Long routeId;
    private String transportType;
    private Float occupancyPercentage;
}