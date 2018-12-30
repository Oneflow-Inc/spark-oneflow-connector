name := "spark-oneflow-connector"

version := "0.1.0"

scalaVersion := "2.11.11"

organization := "org.oneflow"

lazy val versions = new {
  val hadoop = "2.7.0"
  val spark = "2.4.0"
}

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % versions.hadoop % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % versions.hadoop % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.apache.spark" %% "spark-sql" % versions.spark % "provided"
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll
)
