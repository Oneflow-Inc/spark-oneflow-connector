name := "spark-oneflow-connector"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.11"

organization := "org.oneflow"

lazy val versions = new {
  val hadoop = "2.7.7"
  val spark = "2.4.4"
}

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % versions.hadoop % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % versions.hadoop % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.apache.spark" %% "spark-sql" % versions.spark % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "org.apache.spark" %% "spark-mllib" % versions.spark % "provided" exclude ("com.google.protobuf", "protobuf-java"),
  "com.google.flatbuffers" % "flatbuffers-java" % "1.11.0-SNAPSHOT",
  "org.oneflow" % "onerec-flatbuffers-java" % "0.1-SNAPSHOT"
)

//libraryDependencies ++= Seq(
//  "org.apache.hadoop" % "hadoop-common" % versions.hadoop ,
//  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % versions.hadoop ,
//  "org.apache.spark" %% "spark-sql" % versions.spark,
//  "org.apache.spark" %% "spark-mllib" % versions.spark,
//  "com.google.flatbuffers" % "flatbuffers-java" % "1.11.0-SNAPSHOT",
//  "org.oneflow" % "onerec-flatbuffers-java" % "0.1-SNAPSHOT"
//)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll
)
