package tp3.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RouteOccupancy {
    private long routeId;
    private double occupancy;
}
