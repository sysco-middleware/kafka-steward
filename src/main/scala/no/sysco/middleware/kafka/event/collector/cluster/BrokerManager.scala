package no.sysco.middleware.kafka.event.collector.cluster

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.opencensus.scala.Stats
import io.opencensus.scala.stats.Measurement
import no.sysco.middleware.kafka.event.collector.internal.EventRepository.DescribeConfig
import no.sysco.middleware.kafka.event.collector.internal.Parser._
import no.sysco.middleware.kafka.event.collector.internal.EventRepository
import no.sysco.middleware.kafka.event.collector.model._
import no.sysco.middleware.kafka.event.proto.collector.{BrokerCreated, BrokerEvent, BrokerUpdated}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

object BrokerManager {
  def props(
    eventRepository: ActorRef,
    eventProducer: ActorRef)(implicit executionContext: ExecutionContext): Props =
    Props(new BrokerManager(eventRepository, eventProducer))

  case class ListBrokers()

}

/**
 * Manage Cluster Nodes state.
 *
 * @param eventProducer Reference to producer, to publish events.
 */
class BrokerManager(eventRepository: ActorRef, eventProducer: ActorRef)(implicit executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  import BrokerManager._
  import no.sysco.middleware.kafka.event.collector.metrics.Metrics._

  var brokers: Map[String, Broker] = Map()

  implicit val timeout: Timeout = 5 seconds

  override def receive(): Receive = {
    case nodesDescribed: NodesDescribed => handleNodesDescribed(nodesDescribed)
    case brokerEvent: BrokerEvent         => handleBrokerEvent(brokerEvent)
    case ListBrokers()                    => handleListBrokers()
  }

  def handleNodesDescribed(nodesDescribed: NodesDescribed): Unit = {
    log.info("Handling {} nodes described event.", nodesDescribed.nodes.size)
    evaluateCurrentNodes(brokers.values.map(_.node).toList, nodesDescribed.nodes)
    evaluateNodesDescribed(nodesDescribed.nodes)
  }

  private def evaluateCurrentNodes(currentBrokers: List[Node], nodes: List[Node]): Unit = {
    currentBrokers match {
      case Nil =>
      case node :: ns =>
        if (!nodes.contains(node)) {
          log.warning("{} is not listed", node)
        }
        evaluateCurrentNodes(ns, nodes)
    }
  }

  private def evaluateNodesDescribed(listedNodes: List[Node]): Unit = {
    listedNodes match {
      case Nil =>
      case node :: ns =>
        val brokerId = String.valueOf(node.id)
        brokers.get(brokerId) match {
          case None =>
            val configFuture =
              (eventRepository ? DescribeConfig(EventRepository.ResourceType.Broker, brokerId)).mapTo[ConfigDescribed]
            configFuture onComplete {
              case Success(configDescribed) =>
                Stats.record(
                  List(nodeTypeTag, createdOperationTypeTag),
                  Measurement.double(totalMessageProducedMeasure, 1))
                eventProducer !
                  BrokerEvent(brokerId, BrokerEvent.Event.BrokerCreated(BrokerCreated(Some(toPb(node)), Some(toPb(configDescribed.config)))))
              case Failure(t) => log.error(t, "Error querying config")
            }
          case Some(thisBroker) =>
            if (!thisBroker.node.equals(node)) {
              val configFuture =
                (eventRepository ? DescribeConfig(EventRepository.ResourceType.Broker, brokerId)).mapTo[ConfigDescribed]
              configFuture onComplete {
                case Success(configDescribed) =>
                  Stats.record(
                    List(nodeTypeTag, updatedOperationTypeTag),
                    Measurement.double(totalMessageProducedMeasure, 1))
                  eventProducer !
                    BrokerEvent(brokerId, BrokerEvent.Event.BrokerUpdated(BrokerUpdated(Some(toPb(node)), Some(toPb(configDescribed.config)))))
                case Failure(t) => log.error(t, "Error querying config")
              }
            }
        }
        evaluateNodesDescribed(ns)
    }
  }

  def handleBrokerEvent(brokerEvent: BrokerEvent): Unit = {
    log.info("Handling node {} event.", brokerEvent.id)
    val brokerId = String.valueOf(brokerEvent.id)
    brokerEvent.event match {
      case event if event.isBrokerCreated =>
        Stats.record(
          List(nodeTypeTag, createdOperationTypeTag),
          Measurement.double(totalMessageConsumedMeasure, 1))
        event.brokerCreated match {
          case Some(brokerCreated) =>
            brokers = brokers + (brokerId -> Broker(brokerId, fromPb(brokerCreated.getNode)))
          case None =>
        }
      case event if event.isBrokerUpdated =>
        Stats.record(
          List(nodeTypeTag, updatedOperationTypeTag),
          Measurement.double(totalMessageConsumedMeasure, 1))
        event.brokerUpdated match {
          case Some(brokerUpdated) =>
            brokers = brokers + (brokerId -> Broker(brokerId, fromPb(brokerUpdated.getNode)))
          case None =>
        }
    }
  }

  def handleListBrokers(): Unit = sender() ! Brokers(brokers.values.toList)

}
