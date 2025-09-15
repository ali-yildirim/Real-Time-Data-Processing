# Real-Time Data Procesing Pipeline

This project demonstrates a real-time data processing pipeline using Apache Kafka, Apache Spark, MongoDB, all orchestrated with Docker. The pipeline fetches data from the Random Data API, processes it using Spark, stores the processed data in MongoDB.


## Project Objectives

- Set up a real-time data ingestion system using Apache Kafka
- Process streaming data in real-time using Apache Spark
- Store processed data in a MongoDB databsase
- Orchestrate the entire pipeline using Docker


## Project Architecture

The project consists of the following components:

- Kafka Producer: A Python script that fetches real-time data from the Random Data API and publishes it to a Kafka topic.
- Kafka: A distributed streaming platform that ingests real-time data from the Kafka Producer and makes it available for processing.
- Spark: A distributed computing system that consumes data from Kafka, processes it in real-time, and stores the processed data in a MongoDB database.
- MongoDB: A No-SQL database management system used to store the processed data.

The project uses `requirements.txt` files to manage the Python dependencies for the Kafka producer and Spark processing scripts. The dependencies are installed within the respective Docker containers during the build process.
