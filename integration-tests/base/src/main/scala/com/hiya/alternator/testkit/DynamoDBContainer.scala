package com.hiya.alternator.testkit

import com.dimafeng.testcontainers.LocalStackV2Container
import org.testcontainers.containers.localstack.LocalStackContainer

/** Testcontainers-scala wrapper for LocalStack with DynamoDB service.
  *
  * Uses the built-in LocalStackV2Container module for proper LocalStack integration. LocalStack provides a local AWS
  * cloud stack including DynamoDB and other services. The container exposes services on port 4566 internally, which is
  * mapped to a dynamic host port to avoid conflicts.
  */
class DynamoDBContainer private (underlying: LocalStackV2Container) {

  /** Returns the DynamoDB endpoint for this LocalStack container.
    *
    * Uses LocalStack's service-specific endpoint resolution.
    */
  def endpoint: String =
    underlying.endpointOverride(LocalStackContainer.Service.DYNAMODB).toString

  /** Returns the dynamically allocated host port for LocalStack (4566).
    */
  def mappedPort: Int = underlying.mappedPort(4566)

  /** Stops the container and cleans up resources.
    */
  def stop(): Unit = underlying.stop()
}

object DynamoDBContainer {

  /** Container definition for LocalStack with DynamoDB service.
    *
    * Uses the latest LocalStack image and enables only the DynamoDB service for optimal startup time and resource
    * usage.
    */
  val containerDef: LocalStackV2Container.Def = LocalStackV2Container.Def(
    tag = "latest",
    services = Seq(LocalStackContainer.Service.DYNAMODB)
  )

  /** Creates and starts a new LocalStack container with DynamoDB service.
    *
    * The container is immediately started and ready for connections. Use `container.stop()` to shut down the container
    * when done.
    */
  def apply(): DynamoDBContainer = {
    val underlying = containerDef.start()
    new DynamoDBContainer(underlying)
  }
}
