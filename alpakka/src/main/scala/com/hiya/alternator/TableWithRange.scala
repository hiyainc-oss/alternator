package com.hiya.alternator

import com.hiya.alternator.util._
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import com.hiya.alternator.RKCondition.QueryBuilder
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{QueryRequest, QueryResponse}

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.Try

class TableWithRange[V, PK, RK](name: String, schema: TableSchemaWithRange.Aux[V, PK, RK])
  extends Table[V, (PK, RK)](name, schema) {

  def queryBuilder(pk: PK, rk: RKCondition[RK] = RKCondition.empty, limit: Option[Int] = None): QueryRequest.Builder = {
    rk.render(
      schema.rkField,
      RKCondition.EQ(pk)(schema.PK).render(schema.pkField, QueryBuilder)
    )(
      QueryRequest
        .builder()
        .tableName(name)
        .optApp(_.limit)(limit.map(Int.box))
    )
  }

  final def deserialize(response: QueryResponse): List[Try[V]] = {
    if (response.hasItems) response.items().asScala.toList.map(deserialize)
    else Nil
  }


  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty, limit: Option[Int] = None)(implicit client: DynamoDbAsyncClient, mat: Materializer): Future[List[Try[V]]] = {
    val q = queryBuilder(pk, rk, limit).build()
    println(q)
    DynamoDb.single(q).map(deserialize)(Table.parasitic)
  }
}
