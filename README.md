# Day 19 - Spark Structured Streaming

## Overview
Real-time event processing using Apache Spark Structured Streaming and Apache Kafka.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.9
- Apache Kafka
- Java 17
- sbt

## Features
- Reads JSON events from the recommendation-events Kafka topic
- Parses event data using Spark
- Processes events using 10-second event-time windows
- Groups events by product ID and event type
- Counts events and displays results in the console

## Sample Event
{"customer_id":"C101","product_id":"Laptop","event_type":"view","timestamp":"2026-10-12T07:00:00"}

## Run the Project
1. Start Kafka and ensure the recommendation-events topic exists.
2. Run `sbt compile`.
3. Run `sbt run`.
4. Send JSON events to the Kafka topic.
