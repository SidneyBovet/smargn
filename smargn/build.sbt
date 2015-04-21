name := """smargn"""

version := "1.0"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(jdbc, anorm, cache, ws, "org.apache.spark" %% "spark-core" % "1.2.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test", "org.slf4j" % "slf4j-log4j12" % "1.7.12",
  "com.decodified" %% "scala-ssh" % "0.7.0", "ch.qos.logback" % "logback-classic" % "1.1.2",
  "org.bouncycastle" % "bcprov-jdk16" % "1.46", "com.jcraft" % "jzlib" % "1.1.3")