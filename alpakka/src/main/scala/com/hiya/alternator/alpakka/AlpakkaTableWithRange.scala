package com.hiya.alternator.alpakka

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.hiya.alternator.DynamoFormat
import com.hiya.alternator.crud.TableWithRangeOps
import com.hiya.alternator.syntax.RKCondition

import scala.concurrent.Future

trait AlpakkaTableWithRange[V, PK, RK] extends TableWithRangeOps[V, PK, RK, Future, Source[*, NotUsed]] with AlpakkaTable[V, (PK, RK)] {
  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): Source[DynamoFormat.Result[V], NotUsed]
}


