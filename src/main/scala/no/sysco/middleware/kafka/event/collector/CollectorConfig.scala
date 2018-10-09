package no.sysco.middleware.kafka.event.collector

import java.time.Duration

import com.typesafe.config.Config

import scala.collection.JavaConverters._

class CollectorConfig(config: Config) {
  object Collector {
    val eventTopic: String = config.getString("collector.event-topic")

    object Cluster {
      val pollInterval: Duration = config.getDuration("collector.cluster.poll-interval")
    }

    object Topic {
      val pollInterval: Duration = config.getDuration("collector.topic.poll-interval")
      val includeInternalTopics: Boolean = config.getBoolean("collector.topic.include-internal-topics")
      val whitelist: List[String] = config.getStringList("collector.topic.whitelist").asScala.toList
      val blacklist: List[String] = config.getStringList("collector.topic.blacklist").asScala.toList
    }
  }
  object Kafka {
    val bootstrapServers: String = config.getString("kafka.bootstrap-servers")
  }
}
