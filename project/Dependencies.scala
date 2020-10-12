import Dependencies.scalaTest
import sbt._

object Dependencies {
  private val scalaCheckV = "3.2.2"
  private val akkaV = "2.6.9"
  private val akkaHttpV = "10.2.1"

  private val dynamoDB            = "software.amazon.awssdk"      % "dynamodb"         % "2.15.2"
  private val shapeless           = "com.chuusai"                %% "shapeless"        % "2.3.3"
  private val scanamo             = "org.scanamo"                %% "scanamo"          % "1.0.0-M12-1"
  private val scalaTest           = "org.scalatest"              %% "scalatest"        % scalaCheckV
  private val scalaCheck          = "org.scalatestplus"          %% "scalacheck-1-14"  % s"${scalaCheckV}.0"
  private val cats                = "org.typelevel"              %% "cats-core"        % "2.1.1"
  private val scalaMeter          = "com.storm-enroute"          %% "scalameter"       % "0.19"
  private val akkaActor           = "com.typesafe.akka"          %% "akka-actor"       % akkaV
  private val akkaTyped           = "com.typesafe.akka"          %% "akka-actor-typed" % akkaV
  private val akkaTestkit         = "com.typesafe.akka"          %% "akka-testkit"     % akkaV
  private val scalaCheckShapeless = "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5"
  private val alpakkaDynamoDB     = "com.lightbend.akka"         %% "akka-stream-alpakka-dynamodb" % "2.0.2"
  private val akkaHttp            = "com.typesafe.akka"          %% "akka-http"         % akkaHttpV

  val AttributeValue = Seq(
    dynamoDB,
    shapeless,
    cats,
    akkaActor,
    scanamo             % Test,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaMeter          % Test,
    scalaCheckShapeless % Test
  )

  val Alpakka = Seq(
    alpakkaDynamoDB,
    akkaHttp,
    akkaTyped,
    akkaTestkit         % Test,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaCheckShapeless % Test
  )
}
