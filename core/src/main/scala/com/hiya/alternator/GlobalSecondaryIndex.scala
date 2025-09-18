package com.hiya.alternator

final case class GlobalSecondaryIndex(
  indexName: String,
  partitionKeyName: String,
  rangeKeyName: Option[String] = None
)
