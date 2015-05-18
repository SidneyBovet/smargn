name := "SparkCommander"

version := "1.0"

scalaVersion := "2.10.4"

mainClass in Compile := Some("SparkCommander")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"
)

resolvers += Resolver.sonatypeRepo("public")
