package com.hiya.alternator.aws1

import cats.syntax.all._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, model}
import com.hiya.alternator.internal._
import com.hiya.alternator.TableWithRangeKeyLike
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition}

import scala.jdk.CollectionConverters._

class Aws1TableWithRangeKey[V, PK, RK](val underlying: TableWithRangeKeyLike[AmazonDynamoDBAsync, V, PK, RK])
  extends AnyVal {

  import underlying._

  def query(
    pk: PK,
    rk: RKCondition[RK] = RKCondition.Empty,
    condition: Option[ConditionExpression[_]],
    limit: Option[Int],
    consistent: Boolean
  ): model.QueryRequest = {
    val request: model.QueryRequest =
      new model.QueryRequest(tableName)
        .optApp(_.withLimit)(limit.map(Int.box))
        .withConsistentRead(consistent)

    Condition.eval {
      for {
        keyCondition <- Condition.renderCondition[model.AttributeValue, PK, RK](pk, rk, schema)
        filter <- condition.traverse(Condition.renderCondition(_))
        builder <- Condition.execute(request)
      } yield builder.withKeyConditionExpression(keyCondition).withFilterExpression(filter.orNull)
    }

  }

  final def deserialize(response: model.QueryResult): List[DynamoFormat.Result[V]] = {
    Option(response.getItems).toList.flatMap(_.asScala.toList.map(Aws1Table(underlying).deserialize))
  }
}

object Aws1TableWithRangeKey {
  @inline def apply[V, PK, RK](underlying: TableWithRangeKeyLike[AmazonDynamoDBAsync, V, PK, RK]) =
    new Aws1TableWithRangeKey(underlying)
}
