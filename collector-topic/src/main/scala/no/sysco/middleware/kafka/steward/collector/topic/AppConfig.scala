package no.sysco.middleware.kafka.steward.collector.topic

import java.time.Duration

import com.typesafe.config.Config

object AppConfig {
  def apply(config: Config): AppConfig = new AppConfig(config)
}

class AppConfig(config: Config) {

  object Collector {
    val eventTopic: String = config.getString("collector.topic.event-topic")
    val pollInterval: Duration = config.getDuration("collector.topic.poll-interval")
    val includeInternalTopics: Boolean = config.getBoolean("collector.topic.include-internal-topics")
    val whitelist: List[String] = config.getString("collector.topic.whitelist").split(",").filterNot(s => s.isEmpty).toList
    val blacklist: List[String] = config.getString("collector.topic.blacklist").split(",").filterNot(s => s.isEmpty).toList
  }

  object Kafka {
    val bootstrapServers: String = config.getString("kafka.bootstrap-servers")
  }

}
