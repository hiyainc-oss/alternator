lazy val `alternator-aws2` = LocalProject("alternator-aws2")

lazy val `scanamo-base` = (project in (file("scanamo-base")))
  .enablePlugins(JmhPlugin)
  .dependsOn(`alternator-aws2`)
  .settings(
    BuildCommon.commonSettings,
    publish / skip := true,
    libraryDependencies ++= Dependencies.Tests.ScanamoBase
  )

lazy val `scanamo-aws2-test` = (project in (file("scanamo-aws2")))
  .enablePlugins(JmhPlugin)
  .dependsOn(`scanamo-base`)
  .settings(
    BuildCommon.commonSettings,
    publish / skip := true,
    libraryDependencies ++= Dependencies.Tests.ScanamoAws2
  )

lazy val `alternator-test` = (project in (file("alternator")))
  .enablePlugins(JmhPlugin)
  .dependsOn(`scanamo-base`)
  .settings(
    BuildCommon.commonSettings,
    publish / skip := true,
    libraryDependencies ++= Dependencies.Tests.Alternator
  )


crossScalaVersions := Nil
publish / skip := true
