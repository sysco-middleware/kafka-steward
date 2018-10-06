import sbt._

object Versions {
  val kafka           = "1.0.0"
  val akkaStreams = "2.5.17"
  val akkaHttp = "10.1.5"
  val alpakkaKafka = "0.22"

  val logback         = "1.1.3"
  val opencensus = "0.6.0"

  val scalaTest = "3.0.5"
  val scalaTestEmbeddedKafka = "2.0.0"
}

object Dependencies {
  // core dependencies
  val akkaStreams = "com.typesafe.akka" %% "akka-stream" % Versions.akkaStreams
  val kafkaClients = "org.apache.kafka" % "kafka-clients" % Versions.kafka
  val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka
  val scalaPb = "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"

  // http dependencies
  val akkaHttp = "com.typesafe.akka" %% "akka-http" %  Versions.akkaHttp
  val akkaHttpSpray = "com.typesafe.akka" %% "akka-http-spray-json" % Versions.akkaHttp

  // logging dependencies
  val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % Versions.akkaStreams
  val logback = "ch.qos.logback" % "logback-classic" % Versions.logback % Runtime

  // metrics dependencies
  val opencensus = "com.github.sebruck" %% "opencensus-scala-core" % Versions.opencensus

  // test dependencies
  val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % Test
  val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % Versions.akkaStreams % Test
  val scalaTestEmbeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % Versions.scalaTestEmbeddedKafka % "test"
}

