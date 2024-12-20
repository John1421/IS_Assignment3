# Kafka Streams Project

This project is the third and final assignment for the course Integration Systems at University of Coimbra. It demonstrates the use of Kafka Streams to process and aggregate data related to routes, trips, and other transportation-related metrics. Results are stored in database tables corresponding to specific requirements from the instructions.

## Prerequisites
Before running the project, ensure you have the following installed and configured:

- Docker and Docker Compose (to run the Kafka cluster and database)
- Visual Studio Code with installed Docker extension for easier testing.

## Steps to set up and run the project

1. Build all containers
First, start the Kafka cluster, connectors, and the database:

2. Deploy connectors
Deploy the source and sink connectors for Kafka Connect:

        sh config/create-source.sh
        sh config/create-sink.sh
    These scripts configure connectors to read from and write to the appropriate Kafka topics and database tables.

3. Run the Route producer

    Run the RouteProducer.java file to generate and send route data to the routes-topic. For creating the test data run TestRouteProducer.java

4. Run the Trip producer

    Run the TripProducer.java file to generate and send trip data to the trips-topic. For creating the test data run TestTripProducer.java

5. Start the Streams application
Run the Streams.java file to process and aggregate the data:

6. Verify results
    
    All processed results will be stored in database tables corresponding to each requirement, as follows: req3, req4, req5, ...
    
    You can query the database to view the results.
