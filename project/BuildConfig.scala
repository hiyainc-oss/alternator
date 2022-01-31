import sbt._
import sbt.Keys._

object BuildConfig {
  private val silencerV = "1.7.8"

  private lazy val Silencer = Seq(
    compilerPlugin("com.github.ghik" % "silencer-plugin" % silencerV cross CrossVersion.full),
    "com.github.ghik" % "silencer-lib" % silencerV % Provided cross CrossVersion.full
  )

  lazy val silencerSettings = Seq(
    libraryDependencies ++= Silencer,
    scalacOptions += "-P:silencer:checkUnused",
    scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, p)) if p < 13 => Seq.empty
      case _ => Seq(
        "-P:silencer:lineContentFilters=import scala\\.collection\\.compat\\._"
      )
    })
  )

  lazy val commonSettings = silencerSettings

}
