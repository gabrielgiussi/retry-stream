organization := "oss.ggiussi"
name := "retry-stream"
version := "0.0.1"

scalaVersion := "2.12.8"

val akkaKafkaVersion = "1.0.3"
val scalaTestVersion = "3.0.8"
val jacksonVersion = "2.9.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafkaVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "com.typesafe.akka" %% "akka-stream-kafka-testkit" % akkaKafkaVersion % Test
)