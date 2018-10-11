# Kafka Event Collector

[![Build Status](https://www.travis-ci.org/sysco-middleware/kafka-event-collector.svg?branch=master)](https://www.travis-ci.org/sysco-middleware/kafka-event-collector)

There are many events happening on a Kafka Cluster, as Topics updated, Brokers added to the Cluster, and so on.
This module is designed to expose those events in a Kafka Topics so it can be consumed by other parties.

## How to use it

Protocol to consume these events, following Protocol Buffer format is [documented here](https://github.com/sysco-middleware/kafka-event-collector/wiki/Protocol).

To run this application has 4 parameters to configure:

| Configuration                   | Description                                  | Default Value   | Environmental Variable          |
|---------------------------------|----------------------------------------------|-----------------|---------------------------------|
| kafka.bootstrap-servers         | Kafka Bootstrap Servers address (host:port). | localhost:29092 | KAFKA_BOOTSTRAP_SERVERS         |
| collector.event-topic           | Kafka Topic to store events.                 | __collector     | COLLECTOR_EVENT_TOPIC           |
| collector.topic.poll-interval   | Interval to query Kafka Topics.              | 30 seconds      | COLLECTOR_TOPIC_POLL_INTERVAL   |
| collector.cluster.poll-interval | Interval to query Kafka Cluter.              | 1 minute        | COLLECTOR_CLUSTER_POLL_INTERVAL |

Service:

| Port | Description                                                    |
|------|----------------------------------------------------------------|
| 8080 | HTTP Port including `/topics`, `/cluster`, and `/brokers`      |
| 8081 | Admin Port including Metrics `/metrics` in Prometheus format   |

### SBT

To run locally, `sbt` can be used: 

```bash
sbt run
```

### Docker Compose

To experiment a Docker Compose is defined with a Kafka Broker and Zookeeper: 

```bash
docker-compose up -d
```

### Test

Get Cluster:

```bash
curl http://localhost:8080/cluster | jq .
```

List Brokers:

```bash
curl http://localhost:8080/brokers | jq .
```

List Topics:

```bash
curl http://localhost:8080/topics | jq .
```

Check Metrics:

```bash
curl http://localhost:8081/
```

> It could return empty results until first query is made.
