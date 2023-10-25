lazy val akkaHttpVersion = "10.2.8"
lazy val akkaVersion = "2.6.14"
lazy val circeVersion = "0.14.1"
lazy val AkkaManagementVersion = "1.1.3"

scalaVersion := "2.13.4"
name := "iot-akkacats"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % AkkaManagementVersion,
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
  "com.datastax.oss" % "java-driver-core" % "4.13.0", // See https://github.com/akka/alpakka/issues/2556
  "com.typesafe.akka" %% "akka-persistence-cassandra" % "1.0.5",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "de.heikoseeberger" %% "akka-http-circe" % "1.39.2",
  "ch.qos.logback" % "logback-classic" % "1.2.10",
  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-persistence-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "io.aeron" % "aeron-driver" % "1.37.0",
  "io.aeron" % "aeron-client" % "1.37.0",
  "org.scalatestplus" %% "mockito-4-5" % "3.2.12.0" % Test,
  "org.mockito" %% "mockito-scala" % "1.16.37" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.16.37" % Test
)
