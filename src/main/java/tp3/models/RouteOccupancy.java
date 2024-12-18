package tp3.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RouteOccupancy {
    private Long routeId;
    private Float occupancyPercentage;
}
