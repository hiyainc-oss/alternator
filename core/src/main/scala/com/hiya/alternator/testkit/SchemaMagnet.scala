package com.hiya.alternator.testkit

import com.hiya.alternator.GlobalSecondaryIndex
import com.hiya.alternator.schema.{IndexSchema, IndexSchemaWithRange, ScalarType}

abstract class SchemaMagnet {
  def hashKey: String
  def rangeKey: Option[String]
  def attributes: List[(String, ScalarType)]
  def globalSecondaryIndexes: List[GlobalSecondaryIndex] = Nil

  def withIndex[PK](indexSchema: IndexSchema[_, PK]): SchemaMagnet = {
    val parent = this
    val gsi = GlobalSecondaryIndex(indexSchema.indexName, indexSchema.schema.pkField)
    new SchemaMagnet {
      override def hashKey: String = parent.hashKey
      override def rangeKey: Option[String] = parent.rangeKey
      override def attributes: List[(String, ScalarType)] = (parent.attributes ++ indexSchema.schema.schema).distinct
      override def globalSecondaryIndexes: List[GlobalSecondaryIndex] = parent.globalSecondaryIndexes :+ gsi
    }
  }

  def withIndex[PK, RK](indexSchema: IndexSchemaWithRange[_, PK, RK]): SchemaMagnet = {
    val parent = this
    val gsi = GlobalSecondaryIndex(indexSchema.indexName, indexSchema.schema.pkField, Some(indexSchema.schema.rkField))
    new SchemaMagnet {
      override def hashKey: String = parent.hashKey
      override def rangeKey: Option[String] = parent.rangeKey
      override def attributes: List[(String, ScalarType)] = (parent.attributes ++ indexSchema.schema.schema).distinct
      override def globalSecondaryIndexes: List[GlobalSecondaryIndex] = parent.globalSecondaryIndexes :+ gsi
    }
  }
}

object SchemaMagnet {
  implicit def attributeValue(magnet: (String, ScalarType)): SchemaMagnet =
    new SchemaMagnet {
      override def hashKey: String = magnet._1

      override def rangeKey: Option[String] = None

      override def attributes: List[(String, ScalarType)] = magnet :: Nil
    }

  implicit def attributeValues(magnet: ((String, ScalarType), (String, ScalarType))): SchemaMagnet =
    new SchemaMagnet {
      override def hashKey: String = magnet._1._1

      override def rangeKey: Option[String] = Some(magnet._2._1)

      override def attributes: List[(String, ScalarType)] = magnet._1 :: magnet._2 :: Nil

    }
}
