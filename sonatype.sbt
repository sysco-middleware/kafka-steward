// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "no.sysco.middleware"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// License of your choice
licenses ++= Seq("MIT" -> url("https://github.com/sysco-middleware/kafka-event-collector/blob/master/LICENSE.txt"))

// Where is the source code hosted
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("sysco-middleware", "kafka-event-collector", "noreply@sysco.no"))

developers := List(Developer(id="sysco-middleware",name="Sysco Middleware developers",email="noreply@sysco.no",url=  url("http://sysco.no")))

credentials ++= (for {
  username <- sys.env.get("SONATYPE_USERNAME")
  password <- sys.env.get("SONATYPE_PASSWORD")
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq


//pgpPublicRing := file("./travis/local.pubring.asc")
//pgpSecretRing := file("./travis/local.secring.asc")
//pgpPassphrase := sys.env.get("PGP_PASS").map(_.toCharArray)