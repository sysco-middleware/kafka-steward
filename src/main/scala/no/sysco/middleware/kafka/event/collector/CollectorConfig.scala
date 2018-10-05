package no.sysco.middleware.kafka.event.collector

import java.time.Duration

import com.typesafe.config.Config

class CollectorConfig(config: Config) {
  object Collector {
    val clusterPollInterval: Duration = config.getDuration("collector.cluster.poll-interval")
    val topicPollInterval: Duration = config.getDuration("collector.topic.poll-interval")
    val eventTopic: String = config.getString("collector.event-topic")
  }
  object Kafka {
    val bootstrapServers: String = config.getString("kafka.bootstrap-servers")
  }
}
