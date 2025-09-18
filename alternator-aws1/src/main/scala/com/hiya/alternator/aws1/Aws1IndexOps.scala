package com.hiya.alternator.aws1

import cats.syntax.all._
import com.amazonaws.services.dynamodbv2.model
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.ConditionExpression
import com.hiya.alternator.{DynamoDBOverride, Index}

import scala.jdk.CollectionConverters._

class Aws1IndexOps[V, PK](val underlying: Index[Aws1DynamoDBClient, V, PK])
  extends AnyVal {

  import underlying._

  def query(
    pk: PK,
    condition: Option[ConditionExpression[_]],
    consistent: Boolean,
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): model.QueryRequest = {
    val request = overrides(
      new model.QueryRequest(tableName)
        .withConsistentRead(consistent)
        .withIndexName(underlying.indexName)
    ).asInstanceOf[model.QueryRequest]

    Condition.eval {
      for {
        keyCondition <- Condition.renderPKCondition[model.AttributeValue, PK](pk, underlying.schema)
        filter <- condition.traverse(Condition.renderCondition(_))
        builder <- Condition.execute(request)
      } yield builder.withKeyConditionExpression(keyCondition).withFilterExpression(filter.orNull)
    }

  }

  final def deserialize(response: model.QueryResult): List[DynamoFormat.Result[V]] = {
    Option(response.getItems).toList.flatMap(_.asScala.toList.map(Aws1TableOps(underlying).deserialize))
  }
}

object Aws1IndexOps {
  @inline def apply[V, PK, RK](underlying: Index[Aws1DynamoDBClient, V, PK]) =
    new Aws1IndexOps(underlying)
}
