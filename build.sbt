import Dependencies._
import scalariform.formatter.preferences._

lazy val buildSettings = Seq(
  organization := "no.sysco.middleware.kafka.steward",
  scalaVersion := "2.12.7",
)

lazy val root = (project in file("."))
  .aggregate(api, collector)
  .settings(
    name := "kafka-steward",
    buildSettings
  )
  .enablePlugins(JavaAppPackaging)
  .enablePlugins(DockerPlugin)

lazy val api = (project in file("api"))
  .settings(
    name := "kafka-steward-api",
    buildSettings,
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )

lazy val collector = (project in file("collector"))
  .dependsOn(api)
  .settings(
    name := "kafka-steward-collector",
    buildSettings,
    libraryDependencies ++= Seq(
      akkaStreams,
      alpakkaKafka,
      kafkaClients,

      akkaHttp,
      akkaHttpSpray,

      akkaSlf4j,
      logback,

      opencensus,
      opencensusExporterPrometheus,
      prometheusClientHttpServer,

      scalaTest,
      akkaTestKit,
      scalaTestEmbeddedKafka
    )
  )

lazy val metadata = (project in file("metadata"))
  .dependsOn(api)
  .settings(
    name := "kafka-steward-metadata",
    buildSettings,
    libraryDependencies ++= Seq(
      akkaStreams,
      alpakkaKafka,
      kafkaClients,

      akkaHttp,
      akkaHttpSpray,

      akkaSlf4j,
      logback,

      opencensus,
      opencensusExporterPrometheus,
      prometheusClientHttpServer,

      scalaTest,
      akkaTestKit,
      scalaTestEmbeddedKafka
    )
  )

//mainClass in Compile := Some("no.sysco.middleware.kafka.event.collector.Collector")

parallelExecution in Test := false

//dockerRepository := Some("syscomiddleware")
//dockerUpdateLatest := true
//dockerBaseImage := "openjdk:8-jre-slim"


scalariformPreferences := scalariformPreferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(DanglingCloseParenthesis, Preserve)


// Add the default sonatype repository setting
publishTo := sonatypePublishTo.value