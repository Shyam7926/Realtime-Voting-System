# Realtime-Voting-System
This repository hosts the code for a real-time voting system, developed using Python, Apache Kafka, Spark Streaming, PostgreSQL, and Streamlit. The project leverages Docker Compose to streamline the setup and deployment of all required services within Docker containers.

## Architecture Diagram
![VotingSystem-Voting](https://github.com/user-attachments/assets/35deaf0b-a41e-48a7-8387-d300bc7f0617)

## System Components

- **`main.py`**  
  - Initializes the PostgreSQL database by creating the required tables (`candidates`, `voters`, `votes`).
  - Sets up a Kafka topic and replicates the `votes` table into this topic.
  - Consumes votes from the Kafka topic and produces enriched data to the `voters_topic`.

- **`voting.py`**  
  - Consumes voter information from the Kafka `voters_topic`.
  - Generates voting data and produces it to the `votes_topic` on Kafka.

- **`spark-streaming.py`**  
  - Consumes voting data from the Kafka `votes_topic`.
  - Enriches the data with PostgreSQL records, aggregates votes, and produces the results to specific Kafka topics.

- **`streamlit-app.py`**  
  - Consumes aggregated voting data from Kafka topics and PostgreSQL.
  - Displays real-time voting insights using a Streamlit web application.

## Setting Up the System

This project uses Docker Compose to simplify the setup of Zookeeper, Kafka, and PostgreSQL services.

### Prerequisites

- Python 3.9 or later installed on your machine.
- Docker installed on your machine.
- Docker Compose installed on your machine.

### Steps to Run

1. Clone this repository:
   ```bash
   git clone https://github.com/Shyam7926/Realtime-Voting-System.git
   cd <repository-directory>
   ```
2. Start the services using Docker Compose:
    ```bash
    docker-compose up -d
    ```
    This will spin up the following containers in detached mode:
    - Zookeeper (default port: 2181)
    - Kafka (default port: 9092)
    - PostgreSQL (default port: 5432)
  
### Additional Configuration
If necessary, modify Zookeeper, Kafka, or PostgreSQL configurations, such as exposed ports, by editing the docker-compose.yml file.

Running the Application

1. Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```
2. Create the required tables in PostgreSQL and initialize voter information in a Kafka topic:
    ```bash
    python main.py
    ```
3. Consume voter information from the Kafka topic, generate voting data, and produce it to another Kafka topic:

    ```bash
    python voting.py
    ```
4. Enrich voting data using PostgreSQL, aggregate results, and produce data to specific Kafka topics:

    ```bash
    python spark-streaming.py
    ```
5. Run the Streamlit web app to visualize real-time voting insights:
    ```bash
    streamlit run streamlit-app.py
    ```
### Notes

- Make sure Docker and Docker Compose are running before starting the application.
- Modify configurations in the scripts or docker-compose.yml to suit your environment as needed.
- Enjoy real-time insights into your election data with this scalable voting system!
