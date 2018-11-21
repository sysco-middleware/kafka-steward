package no.sysco.middleware.kafka.steward.collector.topic

import akka.actor.ActorSystem

object Main extends App {

  implicit val system: ActorSystem = ActorSystem("steward-collector-topic")

  val adminClient = ???

  val producer = ???

  val consumer = ???

  system.actorOf(TopicCollector.props(adminClient, producer, consumer))
}
