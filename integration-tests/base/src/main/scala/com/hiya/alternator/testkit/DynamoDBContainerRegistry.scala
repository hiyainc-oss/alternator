package com.hiya.alternator.testkit

/** Thread-safe singleton registry for managing a shared LocalStack container.
  *
  * This registry ensures that only one LocalStack container is created per JVM instance, which is shared across all
  * tests in a module. The container is lazily initialized on first access and automatically shut down when the JVM
  * exits.
  *
  * This approach provides:
  *   - Fast test execution (container started once, reused for all tests)
  *   - Test isolation (tables are created/destroyed per test via LocalDynamoDB.withTable)
  *   - Proper cleanup (shutdown hook ensures container stops on JVM exit)
  */
object DynamoDBContainerRegistry {
  @volatile private var container: Option[DynamoDBContainer] = None
  private val lock = new Object()

  /** Gets or creates the shared LocalStack container with DynamoDB service.
    *
    * On first call, creates a new container, starts it, and registers a shutdown hook for cleanup. Subsequent calls
    * return the same container instance.
    *
    * This method is thread-safe using double-checked locking.
    */
  def getOrCreateContainer(): DynamoDBContainer = {
    container match {
      case Some(c) => c
      case None =>
        lock.synchronized {
          container match {
            case Some(c) => c
            case None =>
              val c = DynamoDBContainer()
              Runtime.getRuntime.addShutdownHook(new Thread(() => c.stop()))
              container = Some(c)
              c
          }
        }
    }
  }

  /** Returns the dynamic port of the shared container.
    *
    * This is the primary method used by LocalDynamoDB to obtain the container port for client configuration.
    */
  def getPort: Int = getOrCreateContainer().mappedPort
}
