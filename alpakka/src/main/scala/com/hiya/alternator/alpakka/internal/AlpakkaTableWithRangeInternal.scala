package com.hiya.alternator.alpakka.internal

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.Source
import com.hiya.alternator.alpakka.AlpakkaTableWithRange
import com.hiya.alternator.syntax.RKCondition
import com.hiya.alternator.{DynamoFormat, TableWithRangeKey}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

class AlpakkaTableWithRangeInternal[V, PK, RK](override val table: TableWithRangeKey[V, PK, RK])(implicit client: DynamoDbAsyncClient, system: ClassicActorSystemProvider)
  extends AlpakkaTableInternal[V, (PK, RK)](table) with AlpakkaTableWithRange[V, PK, RK] {

  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): Source[DynamoFormat.Result[V], NotUsed] = {
    val q = table.queryBuilder(pk, rk).build()
    DynamoDb.source(q).mapConcat(table.deserialize)
  }

}
