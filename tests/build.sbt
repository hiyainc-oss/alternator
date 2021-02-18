lazy val `alternator-attributevalue` = LocalProject("alternator-attributevalue")

lazy val `scanamo-base` = (project in (file("scanamo-base")))
  .enablePlugins(JmhPlugin)
  .dependsOn(`alternator-attributevalue`)
  .settings(
    BuildConfig.commonSettings,
    publish / skip := true,
    libraryDependencies ++= Dependencies.Tests.ScanamoBase
  )

lazy val `scanamo-aws1-test` = (project in (file("scanamo-aws1")))
  .enablePlugins(JmhPlugin)
  .dependsOn(`scanamo-base`)
  .settings(
    BuildConfig.commonSettings,
    publish / skip := true,
    libraryDependencies ++= Dependencies.Tests.ScanamoAws1
  )

lazy val `scanamo-aws2-test` = (project in (file("scanamo-aws2")))
  .enablePlugins(JmhPlugin)
  .dependsOn(`scanamo-base`)
  .settings(
    BuildConfig.commonSettings,
    publish / skip := true,
    libraryDependencies ++= Dependencies.Tests.ScanamoAws2
  )

lazy val `alternator-test` = (project in (file("alternator")))
  .enablePlugins(JmhPlugin)
  .dependsOn(`scanamo-base`)
  .settings(
    BuildConfig.commonSettings,
    publish / skip := true,
    libraryDependencies ++= Dependencies.Tests.Alternator
  )


crossScalaVersions := Nil
publish / skip := true
