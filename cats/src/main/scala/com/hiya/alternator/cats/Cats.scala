package com.hiya.alternator.cats

import cats.effect.Async
import com.hiya.alternator.cats.internal.{CatsTableOpsInternal, CatsTableOpsWithRangeInternal}
import com.hiya.alternator.{Client, Table, TableWithRangeKey}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

class Cats[F[_] : Async](val client: DynamoDbAsyncClient) extends Client {
  override type PKClient[V, PK] = CatsTableOps[F, V, PK]
  override type RKClient[V, PK, RK] = CatsTableOpsWithRange[F, V, PK, RK]

  override def createPkClient[V, PK](table: Table[V, PK]): CatsTableOps[F, V, PK] =
    new CatsTableOpsInternal(table, this)

  override def createRkClient[V, PK, RK](table: TableWithRangeKey[V, PK, RK]): CatsTableOpsWithRange[F, V, PK, RK] =
    new CatsTableOpsWithRangeInternal(table, this)
}


object Cats {
  def apply[F[_] : Async](client: DynamoDbAsyncClient): Cats[F] = new Cats(client)
}
