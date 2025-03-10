package com.hiya.alternator.aws2

import cats.syntax.all._
import com.hiya.alternator.DynamoDBOverride
import com.hiya.alternator.aws2.{Aws2DynamoDBClient, Aws2TableWithRange}
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition}
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.model
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

import scala.jdk.CollectionConverters._

class Aws2TableWithRangeKeyOps[V, PK, RK](val underlying: Aws2TableWithRange[V, PK, RK]) extends AnyVal {

  import underlying._

  def query(
    pk: PK,
    rk: RKCondition[RK] = RKCondition.Empty,
    condition: Option[ConditionExpression[Boolean]],
    consistent: Boolean = false,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): model.QueryRequest.Builder = {
    val request: QueryRequest.Builder = model.QueryRequest
      .builder()
      .tableName(tableName)
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())
      .consistentRead(consistent)

    Condition.eval {
      for {
        keyCondition <- Condition.renderCondition[model.AttributeValue, PK, RK](pk, rk, schema)
        filter <- condition.traverse(Condition.renderCondition(_))
        builder <- Condition.execute(request)
      } yield builder.keyConditionExpression(keyCondition).filterExpression(filter.orNull)
    }
  }

  final def deserialize(response: model.QueryResponse): List[DynamoFormat.Result[V]] = {
    if (response.hasItems) response.items().asScala.toList.map(Aws2TableOps(underlying).deserialize)
    else Nil
  }
}

object Aws2TableWithRangeKeyOps {
  @inline def apply[V, PK, RK](underlying: Aws2TableWithRange[V, PK, RK]) =
    new Aws2TableWithRangeKeyOps(underlying)
}
