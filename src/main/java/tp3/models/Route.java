package tp3.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Route {
    private long id;
    private int passengerCapacity;
    private String origin;
    private String destination;
    private String transportType;
    private String operator;

    // Constructor with random generation
    public Route() {
        this.id = RandomGenerator.getRandomId();
        this.passengerCapacity = RandomGenerator.getRandomCapacity();
        this.origin = RandomGenerator.getRandomCity();
        this.destination = RandomGenerator.getRandomDestination(this.origin);
        this.transportType = RandomGenerator.getRandomTransportType();
        this.operator = RandomGenerator.getRandomOperator();
    }
}
