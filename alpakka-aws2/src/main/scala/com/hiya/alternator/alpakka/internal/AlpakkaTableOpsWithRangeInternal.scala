package com.hiya.alternator.alpakka.internal

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.Source
import com.hiya.alternator.alpakka.{Alpakka, AlpakkaTableOpsWithRange}
import com.hiya.alternator.syntax.RKCondition
import com.hiya.alternator.{DynamoFormat, TableWithRangeKey}

class AlpakkaTableOpsWithRangeInternal[V, PK, RK](override val table: TableWithRangeKey[V, PK, RK], client: Alpakka)(
  implicit system: ClassicActorSystemProvider
) extends AlpakkaTableOpsInternal[V, (PK, RK)](table, client)
  with AlpakkaTableOpsWithRange[V, PK, RK] {

  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): Source[DynamoFormat.Result[V], NotUsed] = {
    val q = table.query(pk, rk).build()
    DynamoDb.source(q).mapConcat(table.deserialize)
  }

}
