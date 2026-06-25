import org.typelevel.sbt.tpolecat.*
import sbt.internal.util.{Appender, GithubAppender}

ThisBuild / crossScalaVersions := Seq("2.13.18", "2.12.19", "3.3.8")
ThisBuild / scalaVersion := "2.13.18"
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
ThisBuild / extraAppenders := {
  val currentAppenders = (ThisBuild / extraAppenders).value
  val baseDir = (ThisBuild / baseDirectory).value
  val isCI = insideCI.value
  (key: Def.ScopedKey[_]) => {
    val baseAppenders = currentAppenders(key)
    if (isCI) {
      new GithubAppender(baseDir) +: baseAppenders
    } else {
      baseAppenders
    }
  }
}

def scala2Deps(scalaVersion: String): Seq[ModuleID] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((2, _)) => Seq(
      "org.scala-lang"              %  "scala-reflect"             % scalaVersion % Provided,
      "com.chuusai"                 %% "shapeless"                 % "2.3.12",
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.16" % "1.3.1" % Test,
    )
    case _ => Seq.empty
  }

lazy val `alternator-core` = (project in file("core"))
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.Core,
    libraryDependencies ++= scala2Deps(scalaVersion.value),
  )


lazy val `alternator-aws2` = (project in file("alternator-aws2"))
  .dependsOn(`alternator-core` % "compile->compile;test->test")
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.AlternatorAws2,
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => Seq(
          "com.github.alexarchambault" %% "scalacheck-shapeless_1.16" % "1.3.1" % Test,
        )
        case _ => Seq.empty
      }
    },
  )

lazy val `alternator-aws1` = (project in file("alternator-aws1"))
  .dependsOn(`alternator-core` % "compile->compile;test->test")
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.AlternatorAws1,
    libraryDependencies ++= scala2Deps(scalaVersion.value),
  )

lazy val `alternator-akka-base` = (project in file("akka-base"))
  .dependsOn(`alternator-core`)
  .settings(
    BuildCommon.commonSettings,
    crossScalaVersions := Seq("2.13.18", "2.12.19"),
    libraryDependencies ++= Dependencies.AkkaBase,
  )

lazy val `alternator-akka-aws2` = (project in file("akka-aws2"))
  .dependsOn(
    `alternator-aws2`,
    `alternator-akka-base`
  )
  .settings(
    BuildCommon.commonSettings,
    crossScalaVersions := Seq("2.13.18", "2.12.19"),
    libraryDependencies ++= Dependencies.AkkaAws2
  )

lazy val `alternator-akka-aws1` = (project in file("akka-aws1"))
  .dependsOn(
    `alternator-aws1`,
    `alternator-akka-base`
  )
  .settings(
    BuildCommon.commonSettings,
    crossScalaVersions := Seq("2.13.18", "2.12.19"),
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

lazy val `alternator-pekko-base` = (project in file("pekko-base"))
  .dependsOn(`alternator-core`)
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.PekkoBase,
  )

lazy val `alternator-pekko-aws2` = (project in file("pekko-aws2"))
  .dependsOn(
    `alternator-aws2`,
    `alternator-pekko-base`
  )
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.PekkoAws2
  )

lazy val `alternator-pekko-aws1` = (project in file("pekko-aws1"))
  .dependsOn(
    `alternator-aws1`,
    `alternator-pekko-base`
  )
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.PekkoAws1
  )

// Integration test subprojects (defined in integration-tests/build.sbt)
lazy val `integration-tests` = project
  .in(file("integration-tests"))
  .settings(
    publish / skip := true,
    Test / skip := BuildCommon.skipIntegrationTests.value
  )

lazy val `tests` = (project in file("tests"))
  .settings(
    crossScalaVersions := Seq("2.13.18", "2.12.19")
  )
publish / skip := true
crossScalaVersions := Nil
