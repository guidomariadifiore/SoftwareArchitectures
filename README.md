How to use:

1) Verify that Docker Desktop and Python are installed on your system
1. docker --version
1. python --version

2) Install the following dependencies
1. pip install paho-mqtt kafka-python

3) Starting the infrastructure
1. Start the Docker Desktop application, open the terminal in the project root folder and run docker-compose up -d --build
1. After a few seconds verify that the containers are all active by running docker ps (you should see mosquitto, kafka-broker, kafka-console, dashboard-service)

4) Open the webpage that tracks alerts
1. Open file dashboard/index.html
1. (optional) To see the Kafka topics in action, open localhost:8080

5) Starting the simulation
1. Each of these commands has to be run in a different terminal window, all opened from the project root folder.
1. To start the simulated data generation, run python smart_gateway.py
1. To start the simulated ingestion layer (MQTT -> Kafka), run python bridge.py
1. To start the simulated Threshold detection service, run python detector.py

6) Testing the simulation
1. To see what would happen in case of an accident, open the file "command.txt" and change its content from "NORMAL" to "CRASH", then save the file
1. Observe how data changes in the different terminal windows, the alert webpage and (optional) Redpanda
1. After you have verified that the crash detection works, you can open "command.txt" and change its content to "NORMAL" again.
1. Observe how the system resumes working normally, and the webpage displays a "everything works normally" page.

1. If you want to test the resilience of the system, manually stop the mqtt service from the Docker Desktop app. You will notice that the smart gateway still saves information on a local buffer. When you start MQTT up again, this information will get sent again.

7) Stopping everyting
1. Close all active terminal windows
1. Open a new terminal and run docker-compose down, or stop the services manually from the Docker Desktop app
