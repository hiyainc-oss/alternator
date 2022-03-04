ThisBuild / crossScalaVersions := Seq("2.13.8", "2.12.15")
ThisBuild / scalaVersion := "2.13.8"
ThisBuild / organization := "com.hiya"
ThisBuild / versionScheme := Some("early-semver")


ThisBuild / githubOwner := "hiyainc-oss"
ThisBuild / githubRepository := "alternator"

ThisBuild / githubWorkflowTargetTags ++= Seq("v*")
ThisBuild / githubWorkflowPublishTargetBranches := Seq(RefPredicate.StartsWith(Ref.Tag("v")))


lazy val `alternator-attributevalue` = (project in file("attributevalue"))
  .settings(
    BuildConfig.commonSettings,
    libraryDependencies ++= Dependencies.AttributeValue,
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided
  )

lazy val `alternator-alpakka` = (project in file("alpakka"))
  .dependsOn(
    `alternator-attributevalue`,
    `alternator-testkit` % Test
  )
  .settings(
    BuildConfig.commonSettings,
    libraryDependencies ++= Dependencies.Alpakka,
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
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

lazy val `alternator-testkit` = (project in file("testkit"))
  .dependsOn(`alternator-attributevalue`)
  .settings(
      BuildConfig.commonSettings,
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
