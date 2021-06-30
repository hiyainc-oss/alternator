package com.hiya.alternator

import akka.NotUsed
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.stream.scaladsl.Source
import com.hiya.alternator.syntax.RKCondition
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{QueryRequest, QueryResponse}

import scala.jdk.CollectionConverters._
import scala.util.Try

class TableWithRange[V, PK, RK](name: String, schema: TableSchemaWithRange.Aux[V, PK, RK])
  extends Table[V, (PK, RK)](name, schema) {

  def queryBuilder(pk: PK, rk: RKCondition[RK] = RKCondition.empty): QueryRequest.Builder = {
    rk.render(
      schema.rkField,
      RKCondition.EQ(pk)(schema.PK).render(schema.pkField, RKCondition.QueryBuilder)
    )(
      QueryRequest
        .builder()
        .tableName(name)
    )
  }

  final def deserialize(response: QueryResponse): List[Try[V]] = {
    if (response.hasItems) response.items().asScala.toList.map(deserialize)
    else Nil
  }

  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty)(implicit client: DynamoDbAsyncClient): Source[Try[V], NotUsed] = {
    val q = queryBuilder(pk, rk).build()
    DynamoDb.source(q).mapConcat(deserialize)
  }
}
