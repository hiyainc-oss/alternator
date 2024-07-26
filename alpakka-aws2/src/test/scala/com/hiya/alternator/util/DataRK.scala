//package aws2.util
//
//import com.hiya.alternator.aws2
//
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
//  implicit val config: TableConfig.Aux[DataRK, (String, String), AlpakkaTableOpsWithRange[DataRK, String, String]] =
//    new TableConfig[DataRK] {
//      override type Key = (String, String)
//      override type TableType = AlpakkaTableOpsWithRange[DataRK, String, String]
//
//
//      override def table(tableName: String, client: DynamoDbAsyncClient)
//                        (implicit system: ClassicActorSystemProvider): AlpakkaTableOpsWithRange[DataRK, String, String] =
//        Table.tableWithRK[DataRK](tableName).withClient(Alpakka(client))
//
//      override def withTable[T](client: DynamoDbAsyncClient)(f: TableType => T)
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
