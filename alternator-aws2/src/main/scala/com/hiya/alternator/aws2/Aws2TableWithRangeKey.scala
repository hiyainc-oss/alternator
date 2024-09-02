package com.hiya.alternator.aws2

import com.hiya.alternator.TableWithRangeKeyLike
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.RKCondition
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model}

import scala.jdk.CollectionConverters._

class Aws2TableWithRangeKey[V, PK, RK](val underlying: TableWithRangeKeyLike[DynamoDbAsyncClient, V, PK, RK])
  extends AnyVal {

  import underlying._

  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): model.QueryRequest.Builder = {
    val q = rk.render(
      schema.rkField,
      RKCondition
        .EQ(pk)(schema.PK)
        .render[model.AttributeValue](
          schema.pkField,
          RKCondition.QueryBuilder()
        )
    )

    model.QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression(q.exp.mkString(" AND "))
      .expressionAttributeNames(q.namesMap.zipWithIndex.map { case (name, idx) => s"#P${idx}" -> name }.toMap.asJava)
      .expressionAttributeValues(
        q.valueMap.zipWithIndex.map { case (name, idx) => s":param${idx}" -> name }.toMap.asJava
      )
  }

  final def deserialize(response: model.QueryResponse): List[DynamoFormat.Result[V]] = {
    if (response.hasItems) response.items().asScala.toList.map(Aws2Table(underlying).deserialize)
    else Nil
  }
}

object Aws2TableWithRangeKey {
  @inline def apply[V, PK, RK](underlying: TableWithRangeKeyLike[DynamoDbAsyncClient, V, PK, RK]) =
    new Aws2TableWithRangeKey(underlying)
}
