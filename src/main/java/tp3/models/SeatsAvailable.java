package tp3.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SeatsAvailable {
    private long routeId;
    private int availableSeats;
    private String operator;
}
