import BuildConfig._


ThisBuild / crossScalaVersions := Seq("2.13.3", "2.12.12")
ThisBuild / scalaVersion := "2.13.3"
ThisBuild / organization := "com.hiya"

lazy val `alternator-attributevalue` = (project in (file("attributevalue")))
  .settings(
    libraryDependencies ++= Dependencies.AttributeValue,
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
  )

lazy val `alternator-alpakka` = (project in (file("alpakka")))
  .dependsOn(`alternator-attributevalue`)
  .settings(
    libraryDependencies ++= Dependencies.Alpakka,
  )

crossScalaVersions := Nil
