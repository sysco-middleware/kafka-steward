import Dependencies._
import scalariform.formatter.preferences._

name := "kafka-event-collector"
organization := "no.sysco.middleware.kafka.event"

lazy val settings = Seq(
  scalaVersion := "2.12.7",
)

lazy val root = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)

libraryDependencies ++= Seq(
  akkaStreams,
  akkaHttp,
  akkaHttpSpray,
  kafkaClients,
  akkaSlf4j,
  logback,
  alpakkaKafka,
  scalaPb
)

libraryDependencies ++= Seq(
  scalaTest,
  akkaTestKit,
  scalaTestEmbeddedKafka
)

mainClass in Compile := Some("no.sysco.middleware.kafka.event.collector.Collector")

parallelExecution in Test := false

dockerRepository := Some("syscomiddleware")
dockerUpdateLatest := true
dockerBaseImage := "openjdk:8-jre-slim"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

scalariformPreferences := scalariformPreferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(DanglingCloseParenthesis, Preserve)


// Add the default sonatype repository setting
publishTo := sonatypePublishTo.value