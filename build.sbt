import BuildConfig._


ThisBuild / crossScalaVersions := Seq("2.13.3", "2.12.12")
ThisBuild / scalaVersion := "2.13.3"
ThisBuild / organization := "com.hiya"

lazy val `alternator-attributevalue` = (project in (file("attributevalue")))
  .settings(
      libraryDependencies ++= Dependencies.AttributeValue,
      libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
  )

//lazy val `alternator-alpakka` = (project in (file("alpakka")))
//  .dependsOn(`alternator-attributevalue`)
//  .settings(
//    libraryDependencies ++= Dependencies.Alpakka,
//    dynamoDBLocalDownloadUrl := Some("https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"),
//    dynamoDBLocalHeapSize := Some(256),
//    dynamoDBLocalPort := 8484,
//    (javaOptions in Test) += s"-DdynamoDBLocalPort=8484",
//    startDynamoDBLocal := startDynamoDBLocal.dependsOn(compile in Test).value,
//    test in Test := (test in Test).dependsOn(startDynamoDBLocal).value,
//    testOnly in Test := (testOnly in Test).dependsOn(startDynamoDBLocal).evaluated,
//    testQuick in Test := (testQuick in Test).dependsOn(startDynamoDBLocal).evaluated,
//    testOptions in Test += dynamoDBLocalTestCleanup.value
//  )

crossScalaVersions := Nil
