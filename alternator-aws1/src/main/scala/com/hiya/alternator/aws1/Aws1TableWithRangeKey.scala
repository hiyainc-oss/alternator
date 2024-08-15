package com.hiya.alternator.aws1

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, model}
import com.hiya.alternator.TableWithRangeLike
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.RKCondition

import scala.jdk.CollectionConverters._

class Aws1TableWithRangeKey[V, PK, RK](val underlying: TableWithRangeLike[AmazonDynamoDBAsync, V, PK, RK])
  extends AnyVal {

  import underlying._

  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): model.QueryRequest = {
    val q = rk.render(
      schema.rkField,
      RKCondition.EQ(pk)(schema.PK).render[model.AttributeValue](
        schema.pkField,
        RKCondition.QueryBuilder()
      )
    )

    new model.QueryRequest(tableName).withKeyConditionExpression(q.exp.mkString(" AND "))
      .withExpressionAttributeNames(q.namesMap.zipWithIndex.map { case (name, idx) => s"#P${idx}" -> name}.toMap.asJava)
      .withExpressionAttributeValues(q.valueMap.zipWithIndex.map { case (name, idx) => s":param${idx}" -> name}.toMap.asJava)
  }

  final def deserialize(response: model.QueryResult): List[DynamoFormat.Result[V]] = {
    Option(response.getItems).toList.flatMap(_.asScala.toList.map(Aws1Table(underlying).deserialize))
  }
}


object Aws1TableWithRangeKey {
  @inline def apply[V, PK, RK](underlying: TableWithRangeLike[AmazonDynamoDBAsync, V, PK, RK]) =
    new Aws1TableWithRangeKey(underlying)
}
