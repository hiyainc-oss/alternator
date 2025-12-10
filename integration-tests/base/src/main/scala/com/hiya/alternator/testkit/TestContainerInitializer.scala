package com.hiya.alternator.testkit

/** Initializes the LocalStack container and sets the system property for DynamoDB port.
  *
  * This trait should be mixed into test base classes to ensure the container is started before any tests run. The
  * initialization happens exactly once per JVM via Scala object initialization semantics.
  */
trait TestContainerInitializer {
  // Force initialization by referencing the object
  TestContainerInitializer.ensureInitialized()
}

object TestContainerInitializer {
  // Eagerly initialize container and set system property
  // Object initializer runs exactly once, thread-safe by JVM guarantees
  private val port: Int = {
    val container = DynamoDBContainerRegistry.getOrCreateContainer()
    val dynamoPort = container.mappedPort
    System.setProperty("dynamoDBLocalPort", dynamoPort.toString)
    dynamoPort
  }

  /** Ensures the container has been initialized. Called by trait to trigger object initialization.
    */
  def ensureInitialized(): Unit = {
    // Access port field to ensure object initialization
    val _ = port
  }
}
