name := "ifood-data-architect-test"

version := "1.0.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.hadoop" % "hadoop-common" % "2.8.4",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.4",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
)

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-" + module.revision + "." + artifact.extension
}