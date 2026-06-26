package com.hiya.alternator.schema

import com.hiya.alternator.generic.util.Exported

private[alternator] trait LowPriorityDynamoFormats {
  final implicit def importedDynamoFormat[A](implicit
    exported: Exported[RootDynamoFormat[A]]
  ): RootDynamoFormat[A] = exported.instance
}
