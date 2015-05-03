name := "SparkCommander"

version := "1.0"

scalaVersion := "2.10.4"

mainClass in Compile := Some("SparkCommander")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

resolvers += Resolver.sonatypeRepo("public")
