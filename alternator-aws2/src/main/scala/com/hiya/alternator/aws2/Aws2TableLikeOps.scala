package com.hiya.alternator.aws2

import com.hiya.alternator._
import com.hiya.alternator.internal._
import com.hiya.alternator.schema.DynamoFormat
import com.hiya.alternator.syntax.{ConditionExpression, Segment}
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

class Aws2TableLikeOps[V, PK](val underlying: TableLike[Aws2DynamoDBClient, V, PK]) extends AnyVal {
  import underlying._

  final def deserialize(response: java.util.Map[String, AttributeValue]): DynamoFormat.Result[V] = {
    schema.serializeValue.readFields(response)
  }

  final def deserialize(response: ScanResponse): List[DynamoFormat.Result[V]] = {
    if (response.hasItems) response.items().asScala.toList.map(deserialize)
    else Nil
  }

  final def scan(
    segment: Option[Segment] = None,
    condition: Option[ConditionExpression[Boolean]],
    consistent: Boolean,
    overrides: DynamoDBOverride.Configure[Aws2DynamoDBClient.OverrideBuilder]
  ): ScanRequest.Builder = {
    val request = ScanRequest
      .builder()
      .tableName(tableName)
      .optApp(_.indexName)(underlying.indexNameOpt)
      .optApp(req => (segment: Segment) => req.segment(segment.segment).totalSegments(segment.totalSegments))(segment)
      .consistentRead(consistent)
      .overrideConfiguration(overrides(AwsRequestOverrideConfiguration.builder()).build())

    condition match {
      case Some(cond) => ConditionalSupport.eval(request, cond)
      case None => request
    }
  }

}

object Aws2TableLikeOps {
  @inline def apply[V, PK](underlying: Aws2TableLike[V, PK]): Aws2TableLikeOps[V, PK] = new Aws2TableLikeOps(underlying)
}
