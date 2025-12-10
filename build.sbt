import org.typelevel.sbt.tpolecat.*

ThisBuild / crossScalaVersions := Seq("2.13.16", "2.12.19")
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / organization := "com.hiya"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / tpolecatDefaultOptionsMode := DevMode
ThisBuild / BuildCommon.skipIntegrationTests := insideCI.value && !sys.env.contains("RUN_INTEGRATION_TESTS")
ThisBuild / publishTo := Some("GitHub Package Registry" at "https://maven.pkg.github.com/hiyainc-oss/alternator")
ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "_",
  sys.env.getOrElse("GITHUB_TOKEN", "")
)
ThisBuild / Test / fork := true
ThisBuild / run / fork := true
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

lazy val `alternator-core` = (project in file("core"))
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.Core,
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided,
  )


lazy val `alternator-aws2` = (project in file("alternator-aws2"))
  .dependsOn(`alternator-core`)
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.AlternatorAws2,
  )

lazy val `alternator-aws1` = (project in file("alternator-aws1"))
  .dependsOn(`alternator-core`)
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.AlternatorAws1,
  )

lazy val `alternator-akka-base` = (project in file("akka-base"))
  .dependsOn(`alternator-core`)
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.AkkaBase,
  )

lazy val `alternator-akka-aws2` = (project in file("akka-aws2"))
  .dependsOn(
    `alternator-aws2`,
    `alternator-akka-base`
  )
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.AkkaAws2
  )

lazy val `alternator-akka-aws1` = (project in file("akka-aws1"))
  .dependsOn(
    `alternator-aws1`,
    `alternator-akka-base`
  )
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.AkkaAws1
  )

lazy val `alternator-cats-base` = (project in file("cats-base"))
  .dependsOn(`alternator-core`)
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.CatsBase
  )

lazy val `alternator-cats-aws2` = (project in file("cats-aws2"))
  .dependsOn(
    `alternator-aws2`,
    `alternator-cats-base`
  )
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.CatsAws2
  )

lazy val `alternator-cats-aws1` = (project in file("cats-aws1"))
  .dependsOn(
    `alternator-aws1`,
    `alternator-cats-base`
  )
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.CatsAws1
  )

// Integration test subprojects (defined in integration-tests/build.sbt)
lazy val `integration-tests` = project in file("integration-tests")

lazy val `tests` = project in file("tests")
publish / skip := true
crossScalaVersions := Nil
