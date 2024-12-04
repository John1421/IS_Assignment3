package tp3.models;

import lombok.Data;

@Data
public class Trip {
    private long id;
    private long routeId;
    private String origin;
    private String destination;
    private long passengerId;
    private String transportType;

    // Constructor with random generation
    public Trip() {
        this.id = RandomGenerator.getRandomId();
        this.routeId = RandomGenerator.getRandomId();
        this.origin = RandomGenerator.getRandomCity();
        this.destination = RandomGenerator.getRandomDestination(this.origin);
        this.passengerId = RandomGenerator.getRandomId();
        this.transportType = RandomGenerator.getRandomTransportType();
    }
}
