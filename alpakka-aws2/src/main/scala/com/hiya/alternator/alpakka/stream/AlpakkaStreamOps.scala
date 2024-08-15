package com.hiya.alternator.alpakka.stream

import com.hiya.alternator.{Table, TableLike, aws2}

class AlpakkaStreamOps[C, V, PK](val table: TableLike[C, V, PK]) {
  import table.schema

  final def readRequest(key: PK): AlpakkaTableOps.ReadRequest[Option[DynamoFormat.Result[V]]] =
    AlpakkaTableOps.ReadRequestWoPT(
      table.tableName -> table.schema.serializePK(key),
      table.deserialize(_: AV)
    )

  final def readRequest[PT](key: PK, pt: PT): AlpakkaTableOps.ReadRequest[(Option[DynamoFormat.Result[V]], PT)] =
    AlpakkaTableOps.ReadRequestWPT(
      table.tableName -> table.schema.serializePK(key),
      table.deserialize(_: AV),
      pt
    )

  final def putRequest[PT](item: V, pt: PT): AlpakkaTableOps.WriteRequest[PT] = {
    val itemValue = table.schema.serializeValue.writeFields(item)
    AlpakkaTableOps.WriteRequest(
      table.tableName -> table.schema.serializePK(table.schema.extract(item)),
      Some(itemValue),
      pt
    )
  }

  final def putRequest(item: V): AlpakkaTableOps.WriteRequest[Done] = {
    putRequest(item, Done)
  }

  final def deleteRequest[T](item: T)(implicit T: ItemMagnet[T, V, PK]): AlpakkaTableOps.WriteRequest[Done] = {
    deleteRequest(item, Done)
  }

  final def deleteRequest[T, PT](item: T, pt: PT)(implicit
    T: ItemMagnet[T, V, PK]
  ): AlpakkaTableOps.WriteRequest[PT] = {
    AlpakkaTableOps.WriteRequest(table.tableName -> table.schema.serializePK(T.key(item)), None, pt)
  }

}
