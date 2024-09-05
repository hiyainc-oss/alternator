package com.hiya.alternator.internal

import scala.jdk.CollectionConverters._

private[alternator] final case class ConditionParameters[AV](
  attributeNames: Map[String, String],
  attributeValues: Map[AV, String]
) {
  def apply[Builder](builder: Builder)(implicit Builder: ConditionalSupport[Builder, AV]): Builder = {
    val r2 = if (this.attributeNames.nonEmpty) {
      Builder.withExpressionAttributeNames(builder, names.asJava)
    } else builder

    if (this.attributeValues.nonEmpty) {
      Builder.withExpressionAttributeValues(r2, values.asJava)
    } else r2
  }

  def names: Map[String, String] = this.attributeNames.map(_.swap)
  def values: Map[String, AV] = this.attributeValues.map(_.swap)
}

private[alternator] object ConditionParameters {
  def empty[AV]: ConditionParameters[AV] = ConditionParameters(Map.empty, Map.empty)
}
