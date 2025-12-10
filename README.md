# Alternator

[![CI](https://github.com/hiyainc-oss/alternator/workflows/Continuous%20Integration/badge.svg)](https://github.com/hiyainc-oss/alternator/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Scala 2.12](https://img.shields.io/badge/scala-2.12-red.svg)](https://www.scala-lang.org/)
[![Scala 2.13](https://img.shields.io/badge/scala-2.13-red.svg)](https://www.scala-lang.org/)

> High-performance, type-safe DynamoDB client for Scala

Alternator is an alternative DynamoDB client for Scala, influenced by [Scanamo](https://scanamo.org). It provides significantly better performance (3-10x for read operations) while maintaining near-complete Scanamo compatibility for AttributeValue mapping.

## Why Alternator?

- 🚀 **Performance**: 3-10x faster than Scanamo for read operations (see [benchmarks](#performance))
- 🔒 **Type-safe**: Compile-time guarantees with `TableSchema` and type-safe condition expressions
- 🎯 **Scanamo-compatible**: Almost perfectly compatible DynamoFormat implementation for easy migration
- ⚡ **Flexible**: Choose your effect system (Akka Streams or Cats Effect)
- 🔧 **Multi-SDK**: Support for both AWS SDK v1 and v2

## Quick Start

### Installation

Alternator is not yet published to Maven Central. To use it:

1. Clone the repository:
```bash
git clone https://github.com/hiyainc-oss/alternator.git
cd alternator
```

2. Publish locally:
```bash
sbt publishLocal
```

3. Add dependency to your project (choose one based on your stack):

```scala
// Cats Effect + AWS SDK v2 (Recommended for new projects)
libraryDependencies += "com.hiya" %% "alternator-cats-aws2" % "0.12.0"

// Cats Effect + AWS SDK v1
libraryDependencies += "com.hiya" %% "alternator-cats-aws1" % "0.12.0"

// Akka Streams + AWS SDK v2
libraryDependencies += "com.hiya" %% "alternator-akka-aws2" % "0.12.0"

// Akka Streams + AWS SDK v1
libraryDependencies += "com.hiya" %% "alternator-akka-aws1" % "0.12.0"
```

> **Note:** Current version is 0.12.0. Check [releases](https://github.com/hiyainc-oss/alternator/releases) for the latest version

### Basic Usage (Cats Effect)

```scala
import cats.effect.{IO, IOApp}
import com.hiya.alternator._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.{RootDynamoFormat, TableSchema}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

// 1. Define your data model
case class User(userId: String, name: String, email: String)

object User {
  // Derive DynamoFormat for serialization
  implicit val format: RootDynamoFormat[User] = semiauto.derive

  // Define table schema with partition key
  implicit val schema: TableSchema.Aux[User, String] =
    TableSchema.schemaWithPK[User, String]("userId", _.userId)
}

object Example extends IOApp.Simple {
  def run: IO[Unit] = {
    // 2. Create DynamoDB client
    val client = DynamoDbAsyncClient.builder().build()

    // 3. Define table
    val userTable = Table.tableWithPK[User]("users").withClient(client)

    // 4. Perform operations
    for {
      // Put an item
      _ <- DB.put(userTable, User("123", "Alice", "alice@example.com"))

      // Get an item
      user <- DB.get(userTable, "123")
      _ <- IO.println(s"Found user: $user")

      // Delete an item
      _ <- DB.delete(userTable, "123")
    } yield ()
  }
}
```

### Basic Usage (Akka Streams)

```scala
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.hiya.alternator._
import com.hiya.alternator.generic.semiauto
import com.hiya.alternator.schema.{RootDynamoFormat, TableSchema}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.{ExecutionContext, Future}

object AkkaExample extends App {
  implicit val system: ActorSystem = ActorSystem("alternator-example")
  implicit val ec: ExecutionContext = system.dispatcher

  val client = DynamoDbAsyncClient.builder().build()
  val userTable = Table.tableWithPK[User]("users").withClient(client)

  val result: Future[Unit] = for {
    // Put an item
    _ <- DB.put(userTable, User("456", "Bob", "bob@example.com")).runWith(Sink.ignore)

    // Get an item
    user <- DB.get(userTable, "456").runWith(Sink.head)
    _ = println(s"Found user: $user")
  } yield ()

  result.onComplete(_ => system.terminate())
}
```

## Choosing a Module

| Your Stack | AWS SDK | Module to Use |
|------------|---------|---------------|
| New project with Cats Effect | v2 (Recommended) | `alternator-cats-aws2` |
| Existing Cats Effect project | v1 | `alternator-cats-aws1` |
| New project with Akka Streams | v2 (Recommended) | `alternator-akka-aws2` |
| Existing Akka project | v1 | `alternator-akka-aws1` |

> **Note:** AWS SDK v2 is recommended for new projects (better performance, HTTP/2 support, more active development)

## Core Features

- **Type-safe schemas**: Define table structure with compile-time guarantees using `TableSchema`
- **Automatic serialization**: Derive `DynamoFormat` for case classes with Shapeless-based generic derivation
- **Conditional operations**: Type-safe condition expressions for put and delete operations
- **Batch operations**: Efficient batch reads and writes with automatic batching
- **Secondary indexes**: Query and scan operations with Global and Local Secondary Index support
- **Streaming**: Memory-efficient streaming with fs2 (Cats Effect) or Akka Streams
- **Range key support**: Full support for composite keys (partition key + range key)

## Performance

Alternator provides significant performance improvements over Scanamo, particularly for read operations:

| ops/s | Alternator | Scanamo 1.0.0-M12-1 | Scanamo 1.0.0-M15 | Speedup (vs M15) |
|-------|------------|---------------------|-------------------|------------------|
| read_1 | 1,918,044 | 454,412 | 515,801 | **3.7x** |
| read_10 | 204,541 | 50,401 | 42,982 | **4.8x** |
| read_100 | 20,652 | 4,505 | 3,660 | **5.6x** |
| read_1000 | 1,656 | 154 | 136 | **12.2x** |
| read_10000 | 171 | 2 | 2 | **91x** |
| write_1 | 938,950 | 665,111 | 746,414 | 1.3x |
| write_10 | 81,272 | 103,486 | 83,890 | ~1x |
| write_100 | 7,999 | 12,229 | 8,889 | ~1x |
| write_1000 | 771 | 1,191 | 865 | ~1x |
| write_10000 | 73 | 114 | 84 | ~1x |

> **Benchmark environment**: JMH benchmarks measuring operations per second. Higher is better.

## AttributeValue Mapping

Alternator provides an almost perfectly Scanamo-compatible implementation of AttributeValue mapping.

### Compatibility with Scanamo

**Differences from Scanamo:**
- Case objects are only supported within sealed traits (not standalone)
- Binary sets are fully supported: Scanamo doesn't support [withBS](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/AttributeValue.html#withBS-java.nio.ByteBuffer...-), but we map `Set[SdkBytes]`, `Set[ByteBuffer]`, and `Set[Array[Byte]]` to DynamoDB binary sets

**Compatible features:**
- Case class serialization/deserialization
- Nested structures
- Collections (List, Set, Map)
- Optional fields
- Sealed trait hierarchies
- All standard Scala primitives

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Scala Version Support

- Scala 2.12.19
- Scala 2.13.16

## Acknowledgments

- Influenced by [Scanamo](https://github.com/scanamo/scanamo)

---

Copyright 2019-2025 Hiya, Inc.
