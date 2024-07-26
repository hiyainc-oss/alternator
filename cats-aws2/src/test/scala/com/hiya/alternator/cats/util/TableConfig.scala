//package aws2.cats.util
//
//import com.hiya.alternator.aws2
//
//import scala.concurrent.ExecutionContext
//
//abstract class TableConfig[Data, Key, TableType <: CatsTableOps[IO, Data, Key]] {
//  def createData(i: Int, v: Option[Int] = None): (Key, Data)
//  def withTable[T](client: DynamoDbAsyncClient)(f: TableType => T)
//                  (implicit ec: ExecutionContext, system: ClassicActorSystemProvider, timeout: Timeout): T
//  def table(name: String, client: DynamoDbAsyncClient)(implicit system: ClassicActorSystemProvider): TableType
//}
