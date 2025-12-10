import sbt.*

object Dependencies {
  private val akkaV = "2.6.21"
  private val jacksonV = "2.17.2"
  private val testcontainersScalaV = "0.44.0"

  private val dynamoDB2           = "software.amazon.awssdk"      % "dynamodb"         % "2.25.35"
  private val dynamoDB            = "com.amazonaws"               % "aws-java-sdk-dynamodb" % "1.12.765"
  private val shapeless           = "com.chuusai"                %% "shapeless"        % "2.3.12"
  private val scanamoAws2         = "org.scanamo"                %% "scanamo"          % "1.1.1"
  private val scalaTest           = "org.scalatest"              %% "scalatest"        % "3.2.19"
  private val scalaCheck          = "org.scalatestplus"          %% "scalacheck-1-16"  % "3.2.14.0"
  private val cats                = "org.typelevel"              %% "cats-core"        % "2.12.0"
  private val catsEffect          = "org.typelevel"              %% "cats-effect"      % "3.4.2"
  private val catsFree            = "org.typelevel"              %% "cats-free"        % "2.12.0"
  private val fs2Core             = "co.fs2"                     %% "fs2-core"         % "3.10.2"
  private val akkaActor           = "com.typesafe.akka"          %% "akka-actor"       % akkaV
  private val akkaTyped           = "com.typesafe.akka"          %% "akka-actor-typed" % akkaV
  private val akkaTestkit         = "com.typesafe.akka"          %% "akka-testkit"     % akkaV
  private val akkaStream          = "com.typesafe.akka"          %% "akka-stream"      % akkaV
  private val scalaCheckShapeless = "com.github.alexarchambault" %% "scalacheck-shapeless_1.16" % "1.3.1"
  private val collectionsCompat   = "org.scala-lang.modules"     %% "scala-collection-compat" % "2.12.0"
  private val logback             = "ch.qos.logback" % "logback-classic" % "1.5.6"
  private val testcontainersScalaLocalStack = "com.dimafeng" %% "testcontainers-scala-localstack-v2" % testcontainersScalaV
  private val testcontainersScalaScalatest = "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaV

  private val jacksonOverride = Seq(
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV
  )
  
  private val KindProjector = compilerPlugin("org.typelevel" % "kind-projector" % "0.13.3" cross CrossVersion.full)
  private val MonadicFor = compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

  val CompilerPlugins = Seq(KindProjector, MonadicFor)

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

  val AlternatorAws2 = Seq(
    dynamoDB2,
    cats,
    collectionsCompat,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaCheckShapeless % Test
  )

  val AlternatorAws1 = Seq(
    dynamoDB,
    shapeless,
    cats,
    collectionsCompat,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaCheckShapeless % Test
  )

  val Core = Seq(
    shapeless,
    cats,
    catsFree,
    collectionsCompat,
    scalaTest           % Test,
    scalaCheck          % Test,
    scalaCheckShapeless % Test
  )

  val AkkaBase = Seq(
    akkaTyped,
    akkaStream,
    akkaActor,
  )

  val AkkaAws2 = jacksonOverride

  val AkkaAws1 = jacksonOverride

  val CatsBase = Seq(
    catsEffect,
    fs2Core,
  )

  val CatsAws2 = Seq.empty

  val CatsAws1 = Seq.empty

  // Integration test dependencies
  // Note: All integration tests need AWS SDK v2 (dynamoDB2) regardless of which SDK version
  // the module uses, because Testcontainers LocalStack requires it for DynamoDB configuration
  val IntegrationTestBase = Seq(
    dynamoDB2,  // Required by Testcontainers for LocalStack configuration
    testcontainersScalaLocalStack,
    testcontainersScalaScalatest,
    scalaTest,
    scalaCheck,
    scalaCheckShapeless,
    akkaTestkit,
    logback
  )
}
