//import cats.effect._
//import cats.implicits._
//import com.hiya.alternator.DynamoFormat.Result
//import com.hiya.alternator.cats.{Cats, CatsTableOps}
//import com.hiya.alternator.syntax.ConditionExpression
//import com.hiya.alternator.{DynamoFormat, ItemMagnet, Segment, Table}
//import fs2._
//import fs2.interop.reactivestreams._
//import software.amazon.awssdk.services.dynamodb.model.{
//  BatchGetItemResponse,
//  BatchWriteItemResponse,
//  ConditionalCheckFailedException
//}
//
//class CatsTableOpsInternal[F[_]: Async, V, PK](override val table: Table[V, PK], override val client: Cats[F])
//  extends CatsTableOps[F, V, PK] {
//  override def get(pk: PK): F[Option[Result[V]]] = {
//    Async[F]
//      .fromCompletableFuture(Sync[F].delay { client.client.getItem(table.get(pk).build()) })
//      .map(table.deserialize)
//  }
//
//  override def put(value: V): F[Unit] =
//    Async[F]
//      .fromCompletableFuture(Sync[F].delay { client.client.putItem(table.put(value).build()) })
//      .void
//
//  override def put(value: V, condition: ConditionExpression[Boolean]): F[Boolean] =
//    Async[F]
//      .fromCompletableFuture(Sync[F].delay { client.client.putItem(table.put(value, condition).build()) })
//      .map(_ => true)
//      .recover { case _: ConditionalCheckFailedException => false }
//
//  override def delete(key: PK): F[Unit] =
//    Async[F]
//      .fromCompletableFuture(Sync[F].delay { client.client.deleteItem(table.delete(key).build()) })
//      .void
//
//  override def delete(key: PK, condition: ConditionExpression[Boolean]): F[Boolean] =
//    Async[F]
//      .fromCompletableFuture(Sync[F].delay { client.client.deleteItem(table.delete(key, condition).build()) })
//      .map(_ => true)
//      .recover { case _: ConditionalCheckFailedException => false }
//
//  override def scan(segment: Option[Segment]): Stream[F, DynamoFormat.Result[V]] = {
//    client.client
//      .scanPaginator(table.scan(segment).build())
//      .toStreamBuffered(1)
//      .flatMap(data => Stream.emits(table.deserialize(data)))
//  }
//
//  override def batchGet(keys: Seq[PK]): F[BatchGetItemResponse] =
//    Async[F].fromCompletableFuture(Sync[F].delay { client.client.batchGetItem(table.batchGet(keys).build()) })
//
//  override def batchPut(values: Seq[V]): F[BatchWriteItemResponse] =
//    Async[F].fromCompletableFuture(Sync[F].delay { client.client.batchWriteItem(table.batchPut(values).build()) })
//
//  override def batchDelete[T](keys: Seq[T])(implicit T: ItemMagnet[T, V, PK]): F[BatchWriteItemResponse] =
//    Async[F].fromCompletableFuture(Sync[F].delay { client.client.batchWriteItem(table.batchDelete(keys).build()) })
//
//  override def batchWrite(values: Seq[Either[PK, V]]): F[BatchWriteItemResponse] =
//    Async[F].fromCompletableFuture(Sync[F].delay { client.client.batchWriteItem(table.batchWrite(values).build()) })
//}
