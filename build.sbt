import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    organization := "it.unibo",
    name := "collaborative-location-activity-recommendations",
    idePackagePrefix := Some("it.unibo.clar"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "com.github.nscala-time" %% "nscala-time" % "2.30.0"
    ),
    Compile / resourceDirectory := baseDirectory.value / "data",
    Compile / unmanagedResources / includeFilter := "example.csv" | "user0.csv",
    Compile / mainClass := Some("it.unibo.clar.Main"),
    assembly / mainClass := Some("it.unibo.clar.Main"),
    assembly / assemblyJarName := "clar.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "clar"}
  )