name := "homework6"

version := "0.1"

scalaVersion := "2.13.5"

libraryDependencies ++= Seq("org.apache.commons" % "commons-csv" % "1.8",
  "org.apache.kafka" % "kafka-clients" % "2.4.0")
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.14.0-M4")