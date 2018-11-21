package no.sysco.middleware.kafka.steward.collector.topic.infra

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

class OriginSpec
  extends TestKit(ActorSystem("OriginSpec"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with EmbeddedKafka {

  override def afterAll: Unit = TestKit.shutdownActorSystem(system)

  val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  "Origin source and repository" must {
    "store and propagate Topic events" in {
      withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualConfig =>
        //TODO
      }
    }
  }
}
