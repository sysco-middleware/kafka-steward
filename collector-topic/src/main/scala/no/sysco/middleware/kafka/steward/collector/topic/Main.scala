package no.sysco.middleware.kafka.steward.collector.topic

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector
import no.sysco.middleware.kafka.steward.collector.topic.http.HttpTopicCollectorService
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig }
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, ByteArraySerializer }

import scala.concurrent.ExecutionContext

object Main extends App {

  val serviceName = "steward-collector-topic"

  implicit val system: ActorSystem = ActorSystem(serviceName)
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  val appConfig: AppConfig = new AppConfig(ConfigFactory.load())

  try {
    val kafkaClient: Properties = new Properties()
    kafkaClient.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, appConfig.Kafka.bootstrapServers)

    val kafkaAdminClient: Properties = new Properties()
    kafkaAdminClient.putAll(kafkaClient)
    kafkaAdminClient.put(CommonClientConfigs.CLIENT_ID_CONFIG, s"$serviceName-admin")

    val adminClient = AdminClient.create(kafkaAdminClient)

    val kafkaProducerClient: Properties = new Properties()
    kafkaProducerClient.putAll(kafkaClient)
    kafkaProducerClient.put(ProducerConfig.CLIENT_ID_CONFIG, s"$serviceName-producer")
    kafkaProducerClient.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)
    kafkaProducerClient.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new ByteArraySerializer().getClass.getName)
    kafkaProducerClient.put(ProducerConfig.ACKS_CONFIG, "1")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](kafkaProducerClient)

    val kafkaConsumerClient: Properties = new Properties()
    kafkaConsumerClient.putAll(kafkaClient)
    kafkaConsumerClient.put(ConsumerConfig.GROUP_ID_CONFIG, s"$serviceName-consumer")
    kafkaConsumerClient.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new ByteArrayDeserializer().getClass.getName)
    kafkaConsumerClient.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new ByteArrayDeserializer().getClass.getName)
    kafkaConsumerClient.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaConsumerClient)

    val topicCollectorRef =
      system.actorOf(
        TopicCollector.props(
          adminClient,
          producer,
          consumer,
          appConfig.Collector.eventTopic,
          appConfig.Collector.pollInterval), "manager")

    val httpCollectorService = new HttpTopicCollectorService(topicCollectorRef)

    val bindingFuture = Http().bindAndHandle(httpCollectorService.route, "0.0.0.0", 8080)

    PrometheusStatsCollector.createAndRegister()
    val metricsServer = new io.prometheus.client.exporter.HTTPServer(8081)

    sys.addShutdownHook(
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => {
          metricsServer.stop()
          system.terminate()
        }))
  } catch {
    case ex: ConfigException => ex.printStackTrace()
  } finally {
    system.terminate()
  }
}
