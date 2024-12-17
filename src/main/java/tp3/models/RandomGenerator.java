package tp3.models;

import java.util.List;
import java.util.Random;

public class RandomGenerator {
    private static final Random RANDOM = new Random();

    private static final List<String> CITIES = List.of(
            "Coimbra", "Lisboa", "Porto", "Viseu", "Aveiro",
            "Braga", "Faro", "Evora", "Guimaraes", "Leiria",
            "Setubal", "Sintra", "Cascais", "Albufeira", "Madeira");

    private static final List<String> TRANSPORT_TYPES = List.of(
            "Bus", "Train", "Plane", "Car", "Taxi");

    private static List<String> OPERATORS = List.of(
            "SMTUC", "Bolt", "Flixbus", "Rede Expresso");

    public static void setOPERATORS(List<String> operators) {
        OPERATORS = operators;
    }

    public static void addOPERATORS(String operator) {
        OPERATORS.add(operator);
    }

    /**
     * Generate a random city name from the predefined list.
     * 
     * @return a random city name.
     */
    public static String getRandomCity() {
        return CITIES.get(RANDOM.nextInt(CITIES.size()));
    }

    /**
     * Generate a random city name from the predefined list.
     * 
     * @return a random city name.
     */
    public static String getRandomOperator() {
        return OPERATORS.get(RANDOM.nextInt(OPERATORS.size()));
    }

    /**
     * Generate a random destination city that is different from the origin.
     * 
     * @param origin the origin city.
     * @return a random destination city.
     */
    public static String getRandomDestination(String origin) {
        String destination;
        do {
            destination = getRandomCity();
        } while (destination.equals(origin));
        return destination;
    }

    /**
     * Generate a random transport type from the predefined list.
     * 
     * @return a random transport type.
     */
    public static String getRandomTransportType() {
        return TRANSPORT_TYPES.get(RANDOM.nextInt(TRANSPORT_TYPES.size()));
    }

    /**
     * Generate a random positive ID.
     * 
     * @return a random positive long value.
     */
    public static long getRandomId() {
        return Math.abs(RANDOM.nextLong(1000));
    }

    /**
     * Generate a random passenger capacity between 1 and 250.
     * 
     * @return a random integer between 1 and 250.
     */
    public static Long getRandomCapacity() {
        return 1 + RANDOM.nextLong(250);
    }
}
