package no.sysco.middleware.kafka.steward.collector.topic

import java.util.Properties

import akka.actor.ActorSystem
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, ByteArraySerializer }

object Main extends App {

  val serviceName = "steward-collector-topic"

  implicit val system: ActorSystem = ActorSystem(serviceName)

  val config: Config = ConfigFactory.load()

  val appConfig: AppConfig = AppConfig(config)

  val kafkaClient: Properties = new Properties()
  kafkaClient.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, appConfig.Kafka.bootstrapServers)

  val kafkaAdminClient: Properties = new Properties(kafkaClient)
  kafkaAdminClient.put(CommonClientConfigs.CLIENT_ID_CONFIG, s"$serviceName-admin")

  val adminClient = AdminClient.create(kafkaAdminClient)

  val kafkaProducerClient: Properties = new Properties(kafkaClient)
  kafkaProducerClient.put(ProducerConfig.CLIENT_ID_CONFIG, s"$serviceName-producer")
  kafkaProducerClient.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)
  kafkaProducerClient.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)
  kafkaProducerClient.put(ProducerConfig.ACKS_CONFIG, "1")

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](kafkaProducerClient)

  val kafkaConsumerClient: Properties = new Properties(kafkaClient)
  kafkaConsumerClient.put(ConsumerConfig.GROUP_ID_CONFIG, s"$serviceName-consumer")
  kafkaConsumerClient.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new ByteArrayDeserializer().getClass.getName)
  kafkaConsumerClient.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new ByteArrayDeserializer().getClass.getName)
  kafkaConsumerClient.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaConsumerClient)

  system.actorOf(
    TopicCollector.props(
      adminClient,
      producer,
      consumer,
      appConfig.Collector.eventTopic,
      appConfig.Collector.pollInterval))
}
