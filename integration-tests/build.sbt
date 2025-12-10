import org.typelevel.scalacoptions.ScalacOptions

lazy val `integration-tests-base` = (project in file("base"))
  .dependsOn(LocalProject("alternator-core"))
  .settings(
    BuildCommon.commonSettings,
    libraryDependencies ++= Dependencies.IntegrationTestBase,
    tpolecatExcludeOptions += ScalacOptions.warnNonUnitStatement,
    publish / skip := true
  )

lazy val `integration-tests-akka-aws1` = project
  .in(file("akka-aws1"))
  .dependsOn(
    LocalProject("alternator-akka-aws1"),
    `integration-tests-base`
  )
  .settings(
    BuildCommon.commonSettings,
    publish / skip := true,
    Test / skip := BuildCommon.skipIntegrationTests.value
  )

lazy val `integration-tests-akka-aws2` = project
  .in(file("akka-aws2"))
  .dependsOn(
    LocalProject("alternator-akka-aws2"),
    `integration-tests-base`
  )
  .settings(
    BuildCommon.commonSettings,
    publish / skip := true,
    Test / skip := BuildCommon.skipIntegrationTests.value
  )

lazy val `integration-tests-cats-aws1` = project
  .in(file("cats-aws1"))
  .dependsOn(
    LocalProject("alternator-cats-aws1"),
    `integration-tests-base`
  )
  .settings(
    BuildCommon.commonSettings,
    publish / skip := true,
    Test / skip := BuildCommon.skipIntegrationTests.value
  )

lazy val `integration-tests-cats-aws2` = project
  .in(file("cats-aws2"))
  .dependsOn(
    LocalProject("alternator-cats-aws2"),
    `integration-tests-base`
  )
  .settings(
    BuildCommon.commonSettings,
    publish / skip := true,
    Test / skip := BuildCommon.skipIntegrationTests.value
  )

// Root aggregate project
lazy val `integration-tests` = (project in file("."))
  .aggregate(
    `integration-tests-base`,
    `integration-tests-akka-aws1`,
    `integration-tests-akka-aws2`,
    `integration-tests-cats-aws1`,
    `integration-tests-cats-aws2`
  )
  .settings(
    publish / skip := true,
    Test / skip := BuildCommon.skipIntegrationTests.value
  )
