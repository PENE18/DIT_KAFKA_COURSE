name := "kafka-tp-scala"
version := "0.1.0"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  // Kafka clients
  "org.apache.kafka"            %  "kafka-clients" % "3.7.0",

  // JSON avec Circe
  "io.circe"                    %% "circe-core"    % "0.14.7",
  "io.circe"                    %% "circe-generic" % "0.14.7",
  "io.circe"                    %% "circe-parser"  % "0.14.7",

  // Logging
  "ch.qos.logback"              %  "logback-classic"      % "1.4.14",
  "com.typesafe.scala-logging"  %% "scala-logging"        % "3.9.5"
)

// Eviter les conflits de versions sur les dependances transitives
ThisBuild / evictionErrorLevel := Level.Warn

// Configuration pour sbt-assembly (fat JAR)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}
