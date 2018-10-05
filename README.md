# Kafka Event Collector

[![Build Status](https://www.travis-ci.org/sysco-middleware/kafka-event-collector.svg?branch=master)](https://www.travis-ci.org/sysco-middleware/kafka-event-collector)

There are many events happening on a Kafka Cluster, as Topics updated, Brokers added to the Cluster, and so on.
This module is designed to expose those events in a Kafka Topics so it can be consumed by other parties.

## How to run it

There are 3 parameters to configure

| Configuration | Description | Default Value | Environmental Variable |
|---------------|-------------|---------------|------------------------|
| 