resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.5.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.3")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
// https://github.com/scala-ide/scalariform
// https://github.com/sbt/sbt-scalariform
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.2")