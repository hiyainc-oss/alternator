package com.hiya.alternator.aws1

import cats.syntax.all._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, model}
import com.hiya.alternator.TableWithRange
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.{ConditionExpression, RKCondition}

import scala.jdk.CollectionConverters._
import scala.annotation.unused

class Aws1TableWithRangeKeyOps[V, PK, RK](val underlying: TableWithRange[AmazonDynamoDBAsync, V, PK, RK])
  extends AnyVal {

  import underlying._

  def query(
    pk: PK,
    rk: RKCondition[RK] = RKCondition.Empty,
    condition: Option[ConditionExpression[_]],
    consistent: Boolean,
    @unused overrides: Option[Unit] = None
  ): model.QueryRequest = {
    val request: model.QueryRequest =
      new model.QueryRequest(tableName)
        .withConsistentRead(consistent)
        

    Condition.eval {
      for {
        keyCondition <- Condition.renderCondition[model.AttributeValue, PK, RK](pk, rk, underlying.schema)
        filter <- condition.traverse(Condition.renderCondition(_))
        builder <- Condition.execute(request)
      } yield builder.withKeyConditionExpression(keyCondition).withFilterExpression(filter.orNull)
    }

  }

  final def deserialize(response: model.QueryResult): List[DynamoFormat.Result[V]] = {
    Option(response.getItems).toList.flatMap(_.asScala.toList.map(Aws1TableOps(underlying).deserialize))
  }
}

object Aws1TableWithRangeKeyOps {
  @inline def apply[V, PK, RK](underlying: TableWithRange[AmazonDynamoDBAsync, V, PK, RK]) =
    new Aws1TableWithRangeKeyOps(underlying)
}
