import org.typelevel.sbt.tpolecat.*
import org.typelevel.scalacoptions.ScalacOptions

import java.util.Objects

ThisBuild / crossScalaVersions := Seq("2.13.14", "2.12.19")
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.hiya"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / tpolecatDefaultOptionsMode := DevMode
// ThisBuild / githubOwner := "hiyainc-oss"
// ThisBuild / githubRepository := "alternator"
ThisBuild / Test / fork := true
ThisBuild / run / fork := true


def dynamoDBPort(base: Int, name: String, scalaVersion: String): Int =
  Math.abs(base + 31 * Objects.hash(name, scalaVersion)) % 512 + 8484

def withDynamoDBLocal(base: Int = 0) = Seq(
  dynamoDBLocalDownloadUrl := Some("https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"),
  dynamoDBLocalHeapSize := Some(256),
  dynamoDBLocalPort := dynamoDBPort(base, name.value, scalaVersion.value),
  startDynamoDBLocal := startDynamoDBLocal.dependsOn(Test / compile).value,
  Test / test := (Test / test).dependsOn(startDynamoDBLocal).value,
  Test / testOnly := (Test / testOnly).dependsOn(startDynamoDBLocal).evaluated,
  Test / testQuick := (Test / testQuick).dependsOn(startDynamoDBLocal).evaluated,
  Test / testOptions += dynamoDBLocalTestCleanup.value,
  Test / javaOptions += s"-DdynamoDBLocalPort=${dynamoDBPort(base, name.value, scalaVersion.value)}"
)

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(Dependencies.MonadicFor, Dependencies.KindProjector),
  Test / tpolecatExcludeOptions += ScalacOptions.warnNonUnitStatement,
  tpolecatScalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, p)) if p < 13 => Set(
      ScalacOptions.warnOption("conf:cat=unused-locals:s")
    )
    case _ => Set(
      ScalacOptions.warnOption("conf:cat=unused-imports&origin=scala.collection.compat._:s"),
    )
  }),
  tpolecatScalacOptions ++= Set(
//   ScalacOptions.release(javaVersion),
//   ScalacOptions.warnOption("conf:src=src_managed/.*:silent")
  )
) ++ (
  if (CrossVersion.partialVersion(sys.props("java.version")).get._1 > 11) {
    Seq(
      Test / javaOptions ++= Seq(
        // FIX java.lang.reflect.InaccessibleObjectException on JVM > 11
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED"
      )
    )
  } else Seq.empty,
)

lazy val `alternator-core` = (project in file("core"))
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.Core,
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided,
  )

lazy val `test-base` = (project in file("test-base"))
  .dependsOn(`alternator-core`)
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.TestBase,
    tpolecatExcludeOptions += ScalacOptions.warnNonUnitStatement,
    publish / skip := true
  )

lazy val `alternator-aws2` = (project in file("alternator-aws2"))
  .dependsOn(`alternator-core`)
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.AlternatorAws2,
  )

lazy val `alternator-aws1` = (project in file("alternator-aws1"))
  .dependsOn(`alternator-core`)
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.AlternatorAws1,
  )

lazy val `alternator-akka-base` = (project in file("akka-base"))
  .dependsOn(`alternator-core`)
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.AkkaBase,
  )

lazy val `alternator-akka-aws2` = (project in file("akka-aws2"))
  .dependsOn(
    `alternator-aws2`,
    `alternator-akka-base`,
    `test-base` % Test
  )
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.AkkaAws2,
    withDynamoDBLocal()
  )

lazy val `alternator-akka-aws1` = (project in file("akka-aws1"))
  .dependsOn(
    `alternator-aws1`,
    `alternator-akka-base`,
    `test-base` % Test
  )
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.AkkaAws1,
    withDynamoDBLocal()
  )

lazy val `alternator-cats-base` = (project in file("cats-base"))
  .dependsOn(`alternator-core`)
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.CatsBase
  )

lazy val `alternator-cats-aws2` = (project in file("cats-aws2"))
  .dependsOn(
    `alternator-aws2`,
    `alternator-cats-base`,
    `test-base` % Test
  )
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.CatsAws2,
    withDynamoDBLocal()
  )

lazy val `alternator-cats-aws1` = (project in file("cats-aws1"))
  .dependsOn(
    `alternator-aws1`,
    `alternator-cats-base`,
    `test-base` % Test
  )
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.CatsAws1,
    withDynamoDBLocal()
  )

lazy val `tests` = project in file("tests")
publish / skip := true
crossScalaVersions := Nil
