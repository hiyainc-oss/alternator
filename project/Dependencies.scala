import sbt._

object Dependencies {
  private val akkaV = "2.6.20"

  private val dynamoDB2           = "software.amazon.awssdk"      % "dynamodb"         % "2.17.77"
  private val shapeless           = "com.chuusai"                %% "shapeless"        % "2.3.10"
  private val scanamoAws2         = "org.scanamo"                %% "scanamo"          % "1.0.0-M23"
  private val scalaTest           = "org.scalatest"              %% "scalatest"        % "3.2.14"
  private val scalaCheck          = "org.scalatestplus"          %% "scalacheck-1-16"  % "3.2.14.0"
  private val cats                = "org.typelevel"              %% "cats-core"        % "2.9.0"
  private val akkaActor           = "com.typesafe.akka"          %% "akka-actor"       % akkaV
  private val akkaTyped           = "com.typesafe.akka"          %% "akka-actor-typed" % akkaV
  private val akkaTestkit         = "com.typesafe.akka"          %% "akka-testkit"     % akkaV
  private val akkaStream          = "com.typesafe.akka"          %% "akka-stream"      % akkaV
  private val scalaCheckShapeless = "com.github.alexarchambault" %% "scalacheck-shapeless_1.16" % "1.3.1"
  private val alpakkaDynamoDB     = "com.lightbend.akka"         %% "akka-stream-alpakka-dynamodb" % "3.0.4"
  private val collectionsCompat   = "org.scala-lang.modules"     %% "scala-collection-compat" % "2.9.0"
  private val scalaJava8Compat    = "org.scala-lang.modules"     %% "scala-java8-compat"     % "1.0.2"
  private val logback             = "ch.qos.logback" % "logback-classic" % "1.4.5"


  object Tests {
    val ScanamoBase = Seq(
      scalaTest,
      scalaCheck
    )

    val ScanamoAws2 = Seq(
      scanamoAws2
    )

    val Alternator = Seq(
    )
  }

  val AttributeValue = Seq(
    dynamoDB2,
    shapeless,
    cats,
    collectionsCompat,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaCheckShapeless % Test
  )

  val Testkit = Seq(
    dynamoDB2,
    scalaJava8Compat,
  )

  val Alpakka = Seq(
    alpakkaDynamoDB,
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
