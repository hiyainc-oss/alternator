package com.hiya.alternator.aws2

import cats.syntax.all._
import com.hiya.alternator.DynamoDBOverride
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.ConditionExpression
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.model
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

import scala.jdk.CollectionConverters._

class Aws2IndexOps[V, PK](val underlying: Aws2Index[V, PK]) extends AnyVal {

  import underlying._

  def query(
    pk: PK,
    condition: Option[ConditionExpression[Boolean]],
    consistent: Boolean = false,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): model.QueryRequest.Builder = {
    val request: QueryRequest.Builder = model.QueryRequest
      .builder()
      .tableName(tableName)
      .indexName(underlying.indexName)
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())
      .consistentRead(consistent)

    Condition.eval {
      for {
        keyCondition <- Condition.renderPKCondition[model.AttributeValue, PK](pk, schema)
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

object Aws2IndexOps {
  @inline def apply[V, PK, RK](underlying: Aws2Index[V, PK]) =
    new Aws2IndexOps(underlying)
}
