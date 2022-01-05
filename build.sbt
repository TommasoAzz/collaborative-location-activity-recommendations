import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.7"

lazy val root = (project in file("."))
  .settings(
    name := "collaborative-location-activity-recommendations",
    idePackagePrefix := Some("it.unibo.clar"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "com.github.nscala-time" %% "nscala-time" % "2.30.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0"
    )
  )
