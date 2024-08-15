package com.hiya.alternator.testkit

import com.hiya.alternator.schema.ScalarType

abstract class SchemaMagnet {
  def hashKey: String
  def rangeKey: Option[String]
  def attributes: List[(String, ScalarType)]
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
