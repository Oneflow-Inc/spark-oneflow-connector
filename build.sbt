name := "spark-oneflow-connector"

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.12.14"

organization := "org.oneflow"

lazy val versions = new {
  val hadoop = "2.7.0"
  val spark = "3.1.1"
}

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % versions.hadoop % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % versions.hadoop % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.apache.spark" %% "spark-sql" % versions.spark % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.apache.spark" %% "spark-mllib" % versions.spark % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.oneflow" % "onerec-java" % "0.1.0-SNAPSHOT",
  "org.oneflow" % "onerec-generated" % "0.1.0-SNAPSHOT",
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll
)
