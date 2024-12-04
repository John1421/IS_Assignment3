package tp3.models;

import java.util.Random;

import lombok.Data;

@Data
public class Trip {
    long id;
    long route;
    String origin;
    String destination;
    long passengerId;
    String transportType;

    public Trip() {
        Random random = new Random();
        
        this.id = random.nextLong(); // Random long value for id
        this.route = random.nextInt(1000); // Random route number (limited to 0-999 for clarity)
        this.origin = getRandomCity(); // Random city as origin
        this.destination = getRandomCity(); // Random city as destination
        while (this.destination.equals(this.origin)) { // Ensure origin and destination are not the same
            this.destination = getRandomCity();
        }
        this.passengerId = random.nextLong(); // Random long value for passengerId
        this.transportType = getRandomTransportType(); // Random transport type
    }

    // Helper method to generate a random city name
    private String getRandomCity() {
        String[] cities = {"Coimbra", "Lisboa", "Porto", "Viseu", "Aveiro"};
        return cities[new Random().nextInt(cities.length)];
    }

    // Helper method to generate a random transport type
    private String getRandomTransportType() {
        String[] transportTypes = {"Bus", "Train", "Plane", "Car", "Taxi"};
        return transportTypes[new Random().nextInt(transportTypes.length)];
    }

    
}
