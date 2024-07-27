package com.hiya.alternator.testkit

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  KeySchemaElement,
  KeyType,
  ScalarAttributeType
}

abstract class SchemaMagnet {
  def keys: List[KeySchemaElement]
  def attributes: List[AttributeDefinition]
}

object SchemaMagnet {
  implicit def attributeValue(magnet: (String, ScalarAttributeType)): SchemaMagnet =
    new SchemaMagnet {
      override def keys: List[KeySchemaElement] =
        List(KeySchemaElement.builder.attributeName(magnet._1).keyType(KeyType.HASH).build)

      override def attributes: List[AttributeDefinition] =
        List(AttributeDefinition.builder.attributeName(magnet._1).attributeType(magnet._2).build)
    }

  implicit def attributeValues(magnet: ((String, ScalarAttributeType), (String, ScalarAttributeType))): SchemaMagnet =
    new SchemaMagnet {
      override def keys: List[KeySchemaElement] =
        List(
          KeySchemaElement.builder.attributeName(magnet._1._1).keyType(KeyType.HASH).build,
          KeySchemaElement.builder.attributeName(magnet._2._1).keyType(KeyType.RANGE).build
        )

      override def attributes: List[AttributeDefinition] = {
        List(
          AttributeDefinition.builder.attributeName(magnet._1._1).attributeType(magnet._1._2).build,
          AttributeDefinition.builder.attributeName(magnet._2._1).attributeType(magnet._2._2).build
        )
      }
    }
}
