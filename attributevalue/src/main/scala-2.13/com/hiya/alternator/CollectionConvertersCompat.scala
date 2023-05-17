package com.hiya.alternator

private[alternator] object CollectionConvertersCompat {

  def mapAsJava[K, V](map: Map[K, V]): java.util.Map[K, V] =
    scala.jdk.CollectionConverters.MapHasAsJava(map).asJava
}
