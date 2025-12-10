import sbt._
import sbt.Keys._
import org.typelevel.scalacoptions.ScalacOptions
import org.typelevel.sbt.tpolecat.TpolecatPlugin.autoImport._

object BuildCommon {
  // Skip integration tests in CI unless explicitly requested
  lazy val skipIntegrationTests = settingKey[Boolean]("Skip integration tests in CI")

  lazy val commonSettings = Seq(
    libraryDependencies ++= Dependencies.CompilerPlugins,
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
    } else Seq.empty
  )
}
