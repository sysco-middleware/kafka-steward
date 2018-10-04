import sbt._

object Versions {
  val kafka           = "1.0.0"
  val akkaStreams = "2.5.17"
  val alpakkaKafka = "0.22"
  val scalaTest = "3.0.5"
  val logback         = "1.1.3"
  val scalaTestEmbeddedKafka = "2.0.0"
}

object Dependencies {
  val akkaStreams = "com.typesafe.akka" %% "akka-stream" % Versions.akkaStreams

  val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % Versions.akkaStreams
  val akkaSlf4jLogback = "ch.qos.logback" % "logback-classic" % Versions.logback % Runtime

  val kafkaClients = "org.apache.kafka" % "kafka-clients" % Versions.kafka

  val scalaPb = "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
  val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka

  // test dependencies
  val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % Test
  val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % Versions.akkaStreams % Test
  val scalaTestEmbeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % Versions.scalaTestEmbeddedKafka % "test"
}

