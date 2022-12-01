import sbt._
import sbt.Keys._

object BuildConfig {

  lazy val commonSettings = Seq(
    scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, p)) if p < 13 => Seq.empty
      case _ => Seq("-Wconf:cat=unused-imports&origin=scala.collection.compat._:s")
    })
  )


}
