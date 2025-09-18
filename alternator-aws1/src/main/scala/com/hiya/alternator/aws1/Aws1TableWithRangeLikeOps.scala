package com.hiya.alternator.aws1

import cats.syntax.all._
import com.amazonaws.services.dynamodbv2.model
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition}
import com.hiya.alternator.{DynamoDBOverride, TableWithRangeLike}

import scala.jdk.CollectionConverters._

class Aws1TableWithRangeLikeOps[V, PK, RK](val underlying: TableWithRangeLike[Aws1DynamoDBClient, V, PK, RK])
  extends AnyVal {

  import underlying._

  def query(
    pk: PK,
    rk: RKCondition[RK] = RKCondition.Empty,
    condition: Option[ConditionExpression[_]],
    consistent: Boolean,
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): model.QueryRequest = {
    val request = overrides(
      new model.QueryRequest(tableName)
        .withConsistentRead(consistent)
        .optApp(_.withIndexName)(underlying.indexNameOpt)
    ).asInstanceOf[model.QueryRequest]

    Condition.eval {
      for {
        keyCondition <- Condition.renderCondition[model.AttributeValue, PK, RK](pk, rk, underlying.schema)
        filter <- condition.traverse(Condition.renderCondition(_))
        builder <- Condition.execute(request)
      } yield builder.withKeyConditionExpression(keyCondition).withFilterExpression(filter.orNull)
    }

  }

  final def deserialize(response: model.QueryResult): List[DynamoFormat.Result[V]] = {
    Option(response.getItems).toList.flatMap(_.asScala.toList.map(Aws1TableLikeOps(underlying).deserialize))
  }
}

object Aws1TableWithRangeLikeOps {
  @inline def apply[V, PK, RK](underlying: TableWithRangeLike[Aws1DynamoDBClient, V, PK, RK]) =
    new Aws1TableWithRangeLikeOps(underlying)
}
