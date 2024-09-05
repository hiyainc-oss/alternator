package com.hiya.alternator.aws2

import cats.syntax.all._
import com.hiya.alternator.TableWithRangeKeyLike
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition}
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, model}

import scala.jdk.CollectionConverters._

class Aws2TableWithRangeKey[V, PK, RK](val underlying: TableWithRangeKeyLike[DynamoDbAsyncClient, V, PK, RK])
  extends AnyVal {

  import underlying._

  def query(
    pk: PK,
    rk: RKCondition[RK] = RKCondition.Empty,
    condition: Option[ConditionExpression[Boolean]]
  ): model.QueryRequest.Builder = {
    val request: QueryRequest.Builder = model.QueryRequest
      .builder()
      .tableName(tableName)

    Condition.eval {
      for {
        keyCondition <- Condition.renderCondition[model.AttributeValue, PK, RK](pk, rk, schema)
        filter <- condition.traverse(Condition.renderCondition(_))
        builder <- Condition.execute(request)
      } yield builder.keyConditionExpression(keyCondition).filterExpression(filter.orNull)
    }
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
