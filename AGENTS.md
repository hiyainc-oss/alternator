# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

Alternator is an alternative DynamoDB client for Scala, influenced by Scanamo. It provides AttributeValue mapping with near-complete Scanamo compatibility and significantly improved performance (up to 10x for some operations).

### Key Design Principles

- **Type-driven API**: Uses TableSchema to define table structure and key types
- **Multi-backend support**: Supports both AWS SDK v1 and v2
- **Effect system flexibility**: Works with Akka Streams and Cats Effect/fs2
- **Client-agnostic core**: Core logic separated from AWS SDK implementations

## Build Commands

### Running Tests

#### Integration Tests (Testcontainers + LocalStack)

Integration tests use Testcontainers to automatically start a LocalStack container with DynamoDB service. These tests are located in `src/it/scala/` directories and require Docker.

```bash
# Run all integration tests for a specific module
sbt "project alternator-akka-aws2" IntegrationTest/test

# Run a specific integration test class
sbt "project alternator-akka-aws2" "IntegrationTest/testOnly *AkkaAws2WriteTests"

# Run all integration tests across all projects
sbt IntegrationTest/test
```

**Important**:
- **Docker is required** to run integration tests. Ensure Docker is installed and running.
- LocalStack container is automatically started on the first test execution and shared across all tests in a module.
- Each parallel module gets its own container on a different dynamic port to allow parallel testing.
- Containers are cleaned up automatically when the JVM exits.
- LocalStack provides a complete local AWS cloud stack including DynamoDB.

#### Unit Tests

Unit tests (if any) are in `src/test/scala/` and can be run without Docker:

```bash
# Run unit tests for a specific module
sbt "project alternator-akka-aws2" test

# Run all unit tests across all projects
sbt test
```

**Note**: Currently, all existing tests in Alternator are integration tests using LocalStack's DynamoDB service.

### Building

```bash
# Compile test sources
sbt Test/compile

# Compile a specific module
sbt "project alternator-core" Test/compile

# Compile test sources for a specific module
sbt "project alternator-akka-aws2" Test/compile

# Cross-compile for all Scala versions (2.12 and 2.13)
sbt +Test/compile
```

### Formatting

This project uses scalafmt with a strict configuration:

```bash
# Format all Scala files
sbt scalafmtAll

# Check formatting without modifying files
sbt scalafmtCheckAll
```

After editing Scala files, format them with scalafmt.

## Module Architecture

The codebase is organized into multiple SBT modules following a layered architecture:

### Core Layer

- **`alternator-core`**: Pure Scala implementation of DynamoDB schema, AttributeValue mapping (DynamoFormat), and table abstractions. No AWS SDK dependencies. Can be used directly to write generic code that works with any effect system (Akka/Cats) and AWS SDK version (v1/v2).
  - `Table`, `TableWithRange`, `Index`, `IndexWithRange`: Type-safe table definitions
  - `TableSchema`: Defines partition key, range key, and index structure
  - `DynamoFormat`: Type class for marshalling/unmarshalling between Scala types and DynamoDB AttributeValues
  - `DynamoDBClient`: Trait defining the client type (abstract over AWS SDK versions)

### AWS SDK Adapters (Internal)

These modules are internal implementation layers for code sharing, not user-facing APIs:

- **`alternator-aws1`**: AWS SDK v1 (aws-java-sdk-dynamodb) adapter. Provides `Aws1TableOps` with concrete DynamoDB operations.
- **`alternator-aws2`**: AWS SDK v2 (software.amazon.awssdk:dynamodb) adapter. Provides `Aws2TableOps` with concrete DynamoDB operations.

### Effect System Layers

**Internal base modules** (for code sharing):
- **`akka-base`**: Akka-specific batching logic using Akka Typed actors for batch read/write operations.
- **`cats-base`**: Cats Effect specific logic for DynamoDB operations.

**User-facing modules** (use these in your projects):
- **`alternator-akka-aws1`**: Combines akka-base + alternator-aws1. Provides `AkkaAws1` object with DynamoDB operations returning Akka Streams `Source[T, _]`.
- **`alternator-akka-aws2`**: Combines akka-base + alternator-aws2. Provides `AkkaAws2` object with DynamoDB operations returning Akka Streams `Source[T, _]`.
- **`alternator-cats-aws1`**: Combines cats-base + alternator-aws1. Provides Cats Effect IO/fs2 Stream operations.
- **`alternator-cats-aws2`**: Combines cats-base + alternator-aws2. Provides Cats Effect IO/fs2 Stream operations.

### Testing

- **`test-base`**: Shared test utilities and base test traits (e.g., `DynamoDBTestBase`, test data models like `DataPK` and `DataRK`).
- **`tests`**: Aggregate project (publish skipped).

**Pattern**: Each concrete module (e.g., `alternator-akka-aws2`) depends on a base module (e.g., `akka-base`) and an AWS SDK adapter (e.g., `alternator-aws2`), plus `test-base` for testing.

## Key Concepts

### Table Definitions

Tables are defined using `TableSchema` which specifies:
- Partition key (PK): Required for all tables
- Range key (RK): Optional, use `TableSchemaWithRange` for tables with composite keys
- Value type: The case class representing a DynamoDB item

Example:
```scala
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.{RootDynamoFormat, TableSchema}

case class ExampleData(pk: String, intValue: Int, stringValue: String)

implicit val format: RootDynamoFormat[ExampleData] = semiauto.derive
implicit val schema: TableSchema.Aux[ExampleData, String] =
  TableSchema.schemaWithPK[ExampleData, String]("pk", _.pk)
```

### DynamoDB Operations

All operations follow a consistent pattern:
1. Define a `Table` or `TableWithRange` instance with a client
2. Use the effect-specific API (Akka or Cats) to perform operations
3. Operations return either `F[T]` (single item) or `S[T]` (stream) depending on the effect system

Common operations:
- `get`, `put`, `delete`, `query`, `scan`
- Conditional operations: `put`/`delete` with condition expressions
- Return value operations: `putAndReturn`, `deleteAndReturn`
- Batch operations: Automatically handled by Akka actors or Cats/fs2

### Index Support

Secondary indexes are defined using `IndexSchema` or `IndexSchemaWithRange`:
- `IndexSchema`: For indexes with only a partition key
- `IndexSchemaWithRange`: For indexes with both partition and range keys

Indexes are accessed via `table.index(indexSchema)` and support `query` and `scan` operations.

## Testing Alternator Itself

**This section describes how Alternator's internal test suite is structured, not how to test code that uses Alternator.**

**DynamoDBTestBase contains the generic test cases for all implementations.** Each concrete implementation (akka-aws1, akka-aws2, cats-aws1, cats-aws2) extends `DynamoDBTestBase[F, S, C]` and only needs to provide:
- The concrete client instance
- Effect-specific type parameters `F[_]` (single item) and `S[_]` (stream)
- Implementation of `eval` and `list` methods to run effects

The base class provides complete test suites covering:
- Basic CRUD operations (get, put, delete)
- Query operations with all range key conditions (=, <, <=, >, >=, between, beginsWith)
- Scan operations with filtering and limits
- Conditional put/delete with return values
- Secondary index queries and scans (both partition-only and with range keys)

**This means when adding new DynamoDB features, add the test cases once in `DynamoDBTestBase` and all four implementations will automatically test it.**

### Test Infrastructure

Integration tests use Testcontainers to manage LocalStack containers:
- **Location**: Integration tests are in `src/it/scala/` directories (not `src/test/scala/`)
- **Configuration**: Uses sbt's `IntegrationTest` configuration with `Defaults.itSettings`
- **Container**: LocalStack with DynamoDB service (port 4566)
- **Container lifecycle**: One shared container per module, started lazily on first integration test execution
- **Container registry**: `DynamoDBContainerRegistry` provides thread-safe singleton access to the container
- **Test isolation**: Each test creates temporary tables via `LocalDynamoDB.withTable`, ensuring isolation
- **Cleanup**: Containers stop automatically on JVM shutdown; tables are cleaned up after each test
- **Dependencies**: Test dependencies are scoped to `"it,test"` to be available in both configurations

## Testing Code That Uses Alternator

When writing tests for code that uses Alternator, use **Testcontainers with LocalStack** for fast, isolated testing:

### Recommended Approach: Testcontainers

Use the `testcontainers-scala` library with LocalStack container:

```scala
import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

// Start LocalStack container with DynamoDB
val localstackContainer = GenericContainer(
  dockerImage = "localstack/localstack:latest",
  exposedPorts = Seq(4566),
  env = Map("SERVICES" -> "dynamodb", "EAGER_SERVICE_LOADING" -> "1"),
  waitStrategy = Wait.forListeningPort()
)

localstackContainer.start()

// Configure your DynamoDB client to point to the container
val endpoint = s"http://${localstackContainer.host}:${localstackContainer.mappedPort(4566)}"
// Create your client with this endpoint
```

### Test Pattern

```scala
import com.hiya.alternator.testkit.LocalDynamoDB
import com.hiya.alternator.Table

// Define your table schema
val table = Table.tableWithPK[MyData](tableName).withClient(client)

// Use LocalDynamoDB.withTable for isolated test tables
LocalDynamoDB.withTable(client, tableName, LocalDynamoDB.schema[MyData]).eval { _ =>
  // Your test logic here - table is created and will be cleaned up automatically
  for {
    _ <- DB.put(table, myData)
    result <- DB.get(table, key)
  } yield result shouldBe Some(myData)
}
```

### Alternative: Mocking the DynamoDB Client

It is also possible to mock the DynamoDB client (AWS SDK v1 `AmazonDynamoDB` or v2 `DynamoDbClient`) for unit tests:

```scala
// Example with mockito or similar
val mockClient = mock[DynamoDbClient]
when(mockClient.getItem(any())).thenReturn(...)

val table = Table.tableWithPK[MyData](tableName).withClient(mockClient)
```

**Trade-offs**:
- ✅ Faster than DynamoDB Local (no container startup)
- ✅ Good for isolated unit tests of business logic
- ❌ Doesn't test actual DynamoDB behavior, serialization, or query semantics
- ❌ Requires mocking complex AWS SDK responses

### Key Points

- **Preferred**: Use Testcontainers with LocalStack (tests real DynamoDB behavior via LocalStack)
- **Alternative**: Mock the DynamoDB client for fast unit tests (doesn't test actual DynamoDB)
- Alternator's internal tests use Testcontainers with automatic LocalStack container management via `DynamoDBContainerRegistry`
- Each test gets a fresh, isolated table via `LocalDynamoDB.withTable`
- Table schema is automatically derived from your `TableSchema` implicit
- Use `LocalDynamoDB.schema[T].withIndex(indexSchema)` to add secondary indexes for testing
- The `eval` method handles table lifecycle (creation and cleanup)

## Development Workflow

1. **Modify core logic**: Start in `alternator-core` if changing schema or format logic
2. **Modify AWS SDK integration**: Edit `alternator-aws1` or `alternator-aws2` for SDK-specific changes
3. **Modify effect system logic**: Edit `akka-base` or `cats-base` for effect-specific changes
4. **Update tests**: Modify `test-base` for shared test logic, or specific module integration tests in `src/it/scala/`
5. **Run integration tests**: Always run integration tests for the modified module and any dependent modules
6. **Check for errors**: Verify compilation after edits
7. **Format code**: Format modified Scala files with scalafmt

## Adding New Operations

To add a new DynamoDB operation across all four implementations (akka-aws1, akka-aws2, cats-aws1, cats-aws2):

1. **Add to core trait** (`core/src/main/scala/com/hiya/alternator/DynamoDB.scala`):
   - Add the operation signature to the appropriate trait (`DynamoDBSource` for streaming operations or similar)
   - Define the operation using effect-system-agnostic types (`F[_]` for single items, `S[_]` for streams)

2. **Implement AWS SDK operations**:
   - **For AWS SDK v1** (`alternator-aws1`): Add implementation using SDK v1 types and APIs
   - **For AWS SDK v2** (`alternator-aws2`): Add implementation using SDK v2 types and APIs
   - These provide the concrete SDK calls but remain effect-system-agnostic

3. **Implement effect-specific logic**:
   - **For Akka** (`akka-base`, `alternator-akka-aws1`, `alternator-akka-aws2`): Implement streaming with Akka Streams `Source`
   - **For Cats** (`cats-base`, `alternator-cats-aws1`, `alternator-cats-aws2`): Implement with Cats Effect `IO` and fs2 `Stream`

4. **Add tests once in `test-base`**:
   - Add test cases to `DynamoDBTestBase` - all four implementations will automatically inherit and run these tests
   - Each implementation only needs to provide the client instance and `eval`/`list` methods

5. **Verify across all modules**: Run `sbt IntegrationTest/test` to ensure all implementations pass the new tests

## Coding Standards

### Functional Programming Style
- **Prefer immutable data structures**: Use `case class`, immutable collections, and avoid `var`
- **Pure functions**: Functions should be referentially transparent where possible
- **Use Cats/Cats Effect patterns**: Leverage `Monad`, `Traverse`, and other type classes
- **Effect types**: Use `F[_]` for single-value effects, `S[_]` for streaming effects

### Type Annotations
- **Public APIs require explicit return types**: All public methods, traits, and objects must have explicit return types
- **Private/local code**: Type inference is acceptable for private methods, local values, and internal implementations
- **Effect types**: Always annotate effect types (`F[_]`, `IO[_]`, `Source[_, _]`)

### When to Use `F[_]` vs Concrete Types
- **Core logic and AWS SDK adapters**: Use abstract `F[_]` and `S[_]` to remain effect-system-agnostic
- **Effect-specific implementations**: Use concrete types (`Future`, `IO`, `Source`, `Stream`) only in effect-specific modules
- **Tests**: Use the abstract `F[_]` and `S[_]` in `DynamoDBTestBase`, concrete types in implementation-specific test classes

### Pattern Matching
- Use pattern matching extensively for ADTs (sealed traits with case classes/objects)
- Prefer exhaustive pattern matches to avoid runtime errors
- Use `@scala.annotation.tailrec` for recursive functions when appropriate

## Compiler Plugins

This project uses:
- **kind-projector**: Enables `*` syntax for type lambdas
- **better-monadic-for**: Improves for-comprehension desugaring

Both are configured in `project/Dependencies.scala` and applied automatically.

## Cross-Version Support

The project supports Scala 2.12 and 2.13. When making changes:
- **Primary development**: Use Scala 2.13 as the primary development version
- **Cross-compile**: Test with both versions using `+test` or `+compile` before submitting changes
- **Collections**: Collection compatibility differences are handled via `scala-collection-compat`
- **Compiler options**: Version-specific compiler options are configured in `build.sbt`
- **Default version**: Running `sbt compile` or `sbt test` without `+` uses the default version (2.13)
