package no.sysco.middleware.kafka.steward.metadata

import akka.actor.{Actor, ActorLogging, Props}
import no.sysco.middleware.kafka.steward.metadata.model.{Broker, Metadata}
import no.sysco.middleware.kafka.steward.proto.collector.CollectorEvent.Value.BrokerEvent

object BrokerMetadata {
  def props(id: String): Props = Props(new BrokerMetadata(id))
}

class BrokerMetadata(id: String) extends Actor with ActorLogging {

  var broker: Broker = Broker(id)
  var metadata: Metadata = Metadata()

  override def receive: Receive = ???

  private def handleBrokerEvent(brokerEvent: BrokerEvent): Unit = {

  }
}
