# Kafka Event Collector

[![Build Status](https://www.travis-ci.org/sysco-middleware/kafka-event-collector.svg?branch=master)](https://www.travis-ci.org/sysco-middleware/kafka-event-collector)

There are many events happening on a Kafka Cluster, as Topics updated, Brokers added to the Cluster, and so on.
This module is designed to expose those events in a Kafka Topics so it can be consumed by other parties.

## How to run it

There are 4 parameters to configure

| Configuration                   | Description                                  | Default Value   | Environmental Variable          |
|---------------------------------|----------------------------------------------|-----------------|---------------------------------|
| kafka.bootstrap-servers         | Kafka Bootstrap Servers address (host:port). | localhost:29092 | KAFKA_BOOTSTRAP_SERVERS         |
| collector.event-topic           | Kafka Topic to store events.                 | __collector     | COLLECTOR_EVENT_TOPIC           |
| collector.topic.poll-interval   | Interval to query Kafka Topics.              | 1 minute        | COLLECTOR_TOPIC_POLL_INTERVAL   |
| collector.cluster.poll-interval | Interval to query Kafka Cluter.              | 30 minutes      | COLLECTOR_CLUSTER_POLL_INTERVAL |
