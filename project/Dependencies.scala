import sbt._

object Dependencies {
  private val scalaCheckV = "3.2.2"

  private val dynamoDB            = "software.amazon.awssdk"      % "dynamodb"         % "2.15.2"
  private val shapeless           = "com.chuusai"                %% "shapeless"        % "2.3.3"
  private val scanamo             = "org.scanamo"                %% "scanamo"          % "1.0.0-M12-1"
  private val scalaTest           = "org.scalatest"              %% "scalatest"        % scalaCheckV
  private val scalaCheck          = "org.scalatestplus"          %% "scalacheck-1-14"  % s"${scalaCheckV}.0"
  private val cats                = "org.typelevel"              %% "cats-core"        % "2.1.1"
  private val scalaMeter          = "com.storm-enroute"          %% "scalameter"       % "0.19"
  private val actorTyped          = "com.typesafe.akka"          %% "akka-actor-typed" % "2.6.9"
  private val scalaCheckShapeless = "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5"

  val AttributeValue = Seq(
    dynamoDB,
    shapeless,
    cats,
    actorTyped,
    scanamo             % Test,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaMeter          % Test,
    scalaCheckShapeless % Test
  )
}
