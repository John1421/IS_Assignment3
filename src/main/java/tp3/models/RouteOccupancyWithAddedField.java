package tp3.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RouteOccupancyWithAddedField {
    private Long routeId;
    private String name;
    private Float occupancyPercentage;
}