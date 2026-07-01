package com.hiya.alternator.util

import com.hiya.alternator.schema.TableSchema
import com.hiya.alternator.testkit.{LocalDynamoDB, LocalDynamoPartial}
import com.hiya.alternator.{DynamoDBClient, Table}

case class DataNested(key: String, address: DataNested.Address)

object DataNested {
  case class Address(city: String, zip: String)

  import com.hiya.alternator.generic.auto._

  type T[C <: DynamoDBClient] = Table[C, DataNested, String]

  implicit val tableSchemaWithPK: TableSchema.Aux[DataNested, String] =
    TableSchema.schemaWithPK[DataNested, String]("key", _.key)

  implicit val config: TableConfig[DataNested, String, T] =
    new TableConfig[DataNested, String, T] {
      override def table[C <: DynamoDBClient](name: String, client: C): Table[C, DataNested, String] =
        Table.tableWithPK[DataNested](name).withClient(client)

      override def createData(i: Int, v: Option[Int]): (String, DataNested) =
        i.toString -> DataNested(i.toString, Address(s"city-$i", s"zip-${v.getOrElse(i)}"))

      override def withTable[F[_], S[_], C <: DynamoDBClient](
        client: C
      ): LocalDynamoPartial[Table[C, DataNested, String], C] =
        LocalDynamoDB.withRandomTable[C, DataNested](client)
    }
}
