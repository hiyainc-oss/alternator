import sbt._
import sbt.Keys._

object BuildConfig {
  val macroParadiseSettings: Seq[Setting[_]] = Seq(
    Keys.libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n < 13 => Seq(compilerPlugin("org.scalamacros"  % "paradise" % "2.1.1" cross CrossVersion.full))
      case _                      => Seq.empty
    }),
    Keys.scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n < 13 => Seq.empty
      case _ =>  Seq("-Ymacro-annotations")
    })
  )
}
