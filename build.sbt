import Dependencies._
import scalariform.formatter.preferences._

lazy val settings = Seq(
  scalaVersion := "2.12.7",
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "kafka-event-collector",
    organization := "no.sysco.middleware.kafka.event",
  )

libraryDependencies ++= Seq(
  akkaStreams,
  kafkaClients,
  akkaSlf4j,
  akkaSlf4jLogback,
  alpakkaKafka,
  scalaPb
)

libraryDependencies ++= Seq(
  scalaTest,
  akkaTestKit,
  scalaTestEmbeddedKafka
)

parallelExecution in Test := false

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

scalariformPreferences := scalariformPreferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(DanglingCloseParenthesis, Preserve)


// Add the default sonatype repository setting
publishTo := sonatypePublishTo.value