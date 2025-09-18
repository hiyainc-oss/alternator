package com.hiya.alternator.aws1

import com.amazonaws.services.dynamodbv2.model._
import com.hiya.alternator.internal.{ConditionalSupport, OptApp}
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.{ConditionExpression, Segment}
import com.hiya.alternator.{DynamoDBOverride, TableLike}

import scala.jdk.CollectionConverters._

final class Aws1TableLikeOps[V, PK](val underlying: TableLike[_, V, PK]) {
  final def deserialize(response: java.util.Map[String, AttributeValue]): DynamoFormat.Result[V] = {
    underlying.schema.serializeValue.readFields(response)
  }

  final def deserialize(response: ScanResult): List[DynamoFormat.Result[V]] = {
    Option(response.getItems).toList.flatMap(_.asScala.toList.map(deserialize))
  }

  final def scan(
    segment: Option[Segment] = None,
    condition: Option[ConditionExpression[Boolean]],
    consistent: Boolean,
    overrides: DynamoDBOverride.Configure[Aws1DynamoDBClient.OverrideBuilder]
  ): ScanRequest = {
    val request = new ScanRequest(underlying.tableName)
      .optApp(_.withIndexName)(underlying.indexNameOpt)
      .optApp(req =>
        (segment: Segment) => {
          req.withSegment(segment.segment).withTotalSegments(segment.totalSegments)
        }
      )(segment)
      .withConsistentRead(consistent)
      .optApp[ConditionExpression[Boolean]](req => cond => ConditionalSupport.eval(req, cond))(condition)

    overrides(request).asInstanceOf[ScanRequest]
  }
}

object Aws1TableLikeOps {
  @inline def apply[V, PK](underlying: TableLike[_, V, PK]): Aws1TableLikeOps[V, PK] =
    new Aws1TableLikeOps[V, PK](underlying)
}
