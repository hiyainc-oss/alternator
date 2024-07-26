import org.typelevel.sbt.tpolecat._
import org.typelevel.scalacoptions.ScalacOptions

ThisBuild / crossScalaVersions := Seq("2.13.14", "2.12.19")
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.hiya"
ThisBuild / versionScheme := Some("early-semver")

ThisBuild / tpolecatDefaultOptionsMode := DevMode

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
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided
  )

lazy val `alternator-aws2` = (project in file("alternator-aws2"))
  .dependsOn(`alternator-core`)
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.AttributeValue,
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided
  )

lazy val `alternator-alpakka-aws2` = (project in file("alpakka-aws2"))
  .dependsOn(
    `alternator-aws2` % "compile->compile;test->test",
    `alternator-testkit` % Test
  )
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.Alpakka,
    dynamoDBLocalDownloadUrl := Some("https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"),
    dynamoDBLocalHeapSize := Some(256),
    dynamoDBLocalPort := 8484,
    (Test / javaOptions) += s"-DdynamoDBLocalPort=8484",
    startDynamoDBLocal := startDynamoDBLocal.dependsOn(Test / compile).value,
    Test / test := (Test / test).dependsOn(startDynamoDBLocal).value,
    Test / testOnly := (Test / testOnly).dependsOn(startDynamoDBLocal).evaluated,
    Test / testQuick := (Test / testQuick).dependsOn(startDynamoDBLocal).evaluated,
    Test / testOptions += dynamoDBLocalTestCleanup.value,
    Test / fork := true
  )

lazy val `alternator-cats-aws2` = (project in file("cats-aws2"))
  .dependsOn(
    `alternator-aws2` % "compile->compile;test->test",
    `alternator-testkit` % Test
  )
  .settings(
    commonSettings,
    libraryDependencies ++= Dependencies.Cats,
    dynamoDBLocalDownloadUrl := Some("https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"),
    dynamoDBLocalHeapSize := Some(256),
    dynamoDBLocalPort := 8486,
    (Test / javaOptions) += s"-DdynamoDBLocalPort=8486",
    startDynamoDBLocal := startDynamoDBLocal.dependsOn(Test / compile).value,
    Test / test := (Test / test).dependsOn(startDynamoDBLocal).value,
    Test / testOnly := (Test / testOnly).dependsOn(startDynamoDBLocal).evaluated,
    Test / testQuick := (Test / testQuick).dependsOn(startDynamoDBLocal).evaluated,
    Test / testOptions += dynamoDBLocalTestCleanup.value,
    Test / fork := true
  )

lazy val `alternator-testkit` = (project in file("testkit"))
  .dependsOn(`alternator-aws2`)
  .settings(
      commonSettings,
      libraryDependencies ++= Dependencies.Testkit,
      dynamoDBLocalDownloadUrl := Some("https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"),
      dynamoDBLocalHeapSize := Some(256),
      dynamoDBLocalPort := 8485,
      (Test / javaOptions) += s"-DdynamoDBLocalPort=8485",
      startDynamoDBLocal := startDynamoDBLocal.dependsOn(Test / compile).value,
      Test / test := (Test / test).dependsOn(startDynamoDBLocal).value,
      Test / testOnly := (Test / testOnly).dependsOn(startDynamoDBLocal).evaluated,
      Test / testQuick := (Test / testQuick).dependsOn(startDynamoDBLocal).evaluated,
      Test / testOptions += dynamoDBLocalTestCleanup.value,
      Test / fork := true
  )


lazy val `tests` = project in file("tests")

crossScalaVersions := Nil
