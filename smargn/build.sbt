name := """smargn"""

version := "1.0"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(jdbc, anorm, cache, ws, "org.apache.spark" %% "spark-core" % "1.2.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test", "org.slf4j" % "slf4j-log4j12" % "1.7.12")