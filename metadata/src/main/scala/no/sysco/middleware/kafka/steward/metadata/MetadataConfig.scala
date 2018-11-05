package no.sysco.middleware.kafka.steward.metadata

import com.typesafe.config.Config

class MetadataConfig(config: Config) {

  object Metadata {
    val eventTopic: String = config.getString("metadata.event-topic")
  }

  object Kafka {
    val bootstrapServers: String = config.getString("kafka.bootstrap-servers")
  }

}
