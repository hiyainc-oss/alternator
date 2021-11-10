import sbt._

object Dependencies {
  private val scalaCheckV = "3.2.2"
  private val akkaV = "2.6.9"
  private val akkaHttpV = "10.2.1"

  private val dynamoDB2           = "software.amazon.awssdk"      % "dynamodb"         % "2.15.2"
  private val dynamoDB1           = "com.amazonaws"               % "aws-java-sdk-dynamodb" % "1.11.956"
  private val shapeless           = "com.chuusai"                %% "shapeless"        % "2.3.3"
  private val scanamoAws1         = "org.scanamo"                %% "scanamo"          % "1.0.0-M12-1"
  private val scanamoAws2         = "org.scanamo"                %% "scanamo"          % "1.0.0-M15"
  private val scalaTest           = "org.scalatest"              %% "scalatest"        % scalaCheckV
  private val scalaCheck          = "org.scalatestplus"          %% "scalacheck-1-14"  % s"${scalaCheckV}.0"
  private val cats                = "org.typelevel"              %% "cats-core"        % "2.3.1"
  private val akkaActor           = "com.typesafe.akka"          %% "akka-actor"       % akkaV
  private val akkaTyped           = "com.typesafe.akka"          %% "akka-actor-typed" % akkaV
  private val akkaTestkit         = "com.typesafe.akka"          %% "akka-testkit"     % akkaV
  private val akkaStream          = "com.typesafe.akka"          %% "akka-stream"      % akkaV
  private val scalaCheckShapeless = "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5"
  private val alpakkaDynamoDB     = "com.lightbend.akka"         %% "akka-stream-alpakka-dynamodb" % "2.0.2"
  private val akkaHttp            = "com.typesafe.akka"          %% "akka-http"         % akkaHttpV
  private val collectionsCompat   = "org.scala-lang.modules"     %% "scala-collection-compat" % "2.4.2"
  private val scalaJava8Compat    = "org.scala-lang.modules"     %% "scala-java8-compat"     % "0.9.1"
  private val logback             = "ch.qos.logback" % "logback-classic" % "1.2.3"


  object Tests {
    val ScanamoBase = Seq(
      dynamoDB1,
      scalaTest,
      scalaCheck,
      scalaCheckShapeless
    )

    val ScanamoAws1 = Seq(
      scanamoAws1
    )

    val ScanamoAws2 = Seq(
      scanamoAws2
    )

    val Alternator = Seq(
    )
  }

  val AttributeValue = Seq(
    dynamoDB1,
    shapeless,
    cats,
    collectionsCompat,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaCheckShapeless % Test
  )

  val Alpakka = Seq(
    alpakkaDynamoDB,
    akkaHttp,
    akkaTyped,
    akkaStream,
    akkaActor,
    scalaJava8Compat    % Test,
    akkaTestkit         % Test,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaCheckShapeless % Test,
    logback             % Test
  )
}
