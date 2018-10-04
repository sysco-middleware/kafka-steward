package no.sysco.middleware.kafka.event.collector.topic

import java.time.Duration

import com.typesafe.config.Config

class TopicEventCollectorConfig(config: Config) {
  object Collector {
    val pollInterval: Duration = config.getDuration("collector.topic.poll-interval")
    val topicEventTopic: String = config.getString("collector.topic.event-topic")
  }
  object Kafka {
    val bootstrapServers: String = config.getString("kafka.bootstrap-servers")
  }
}
