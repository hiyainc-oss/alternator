package aws2.cats.util

import com.hiya.alternator.aws2

//import java.util.UUID
//import scala.concurrent.ExecutionContext
//
//case class DataRK(key: String, range: String, value: String)
//
//object DataRK {
//  import com.hiya.alternator.generic.auto._
//
//  private implicit val tableSchemaWithRK: TableSchemaWithRange.Aux[DataRK, String, String] =
//    TableSchema.schemaWithRK[DataRK, String, String]("key", "range", x => x.key -> x.range)
//
//  implicit val config: TableConfig[DataRK, (String, String), CatsTableOpsWithRange[IO, DataRK, String, String]] =
//    new TableConfig[DataRK, (String, String), CatsTableOpsWithRange[IO, DataRK, String, String]] {
//      override def table(tableName: String, client: DynamoDbAsyncClient)
//                        (implicit system: ClassicActorSystemProvider): CatsTableOpsWithRange[IO, DataRK, String, String] =
//        Table.tableWithRK[DataRK](tableName).withClient(Cats[IO](client))
//
//      override def withTable[T](client: DynamoDbAsyncClient)(f: CatsTableOpsWithRange[IO, DataRK, String, String] => T)
//                               (implicit ec: ExecutionContext, system: ClassicActorSystemProvider, timeout: Timeout): T = {
//        val tableName = s"test-table-${UUID.randomUUID()}"
//        LocalDynamoDB.withTable(client)(tableName)(LocalDynamoDB.schema[DataRK]) {
//          f(table(tableName, client))
//        }
//      }
//
//      override def createData(i: Int, v: Option[Int]): ((String, String), DataRK) = {
//        val pk = i.toString
//        val rk = if (i % 2 > 0) "a" else "b"
//        pk -> rk -> DataRK(pk, rk, v.getOrElse(i).toString)
//      }
//    }
//}
