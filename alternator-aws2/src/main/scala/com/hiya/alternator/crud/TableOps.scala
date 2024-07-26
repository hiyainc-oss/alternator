package alternator.aws2.crud

//import com.hiya.alternator
//import com.hiya.alternator.aws2._
//import com.hiya.alternator.aws2.schema.DynamoFormat
//import com.hiya.alternator.aws2.syntax.{ConditionExpression, RKCondition, Segment}
//import software.amazon.awssdk.services.dynamodb.model.{BatchGetItemResponse, BatchWriteItemResponse}
//
//trait TableOps[V, PK, Future[_], Source[_]] {
//  val table: Table[V, PK]
//  def client: Client
//
//  def get(pk: PK): Future[Option[DynamoFormat.Result[V]]]
//  def put(value: V): Future[Unit]
//  def put(value: V, condition: ConditionExpression[Boolean]): Future[Boolean]
//  def delete(key: PK): Future[Unit]
//  def delete(key: PK, condition: ConditionExpression[Boolean]): Future[Boolean]
//  def scan(segment: Option[Segment] = None): Source[DynamoFormat.Result[V]]
//
//  def batchGet(values: Seq[PK]): Future[BatchGetItemResponse]
//  def batchPut(values: Seq[V]): Future[BatchWriteItemResponse]
//  def batchDelete[T](values: Seq[T])(implicit T: ItemMagnet[T, V, PK]): Future[BatchWriteItemResponse]
//  def batchWrite(values: Seq[Either[PK, V]]): Future[BatchWriteItemResponse]
//}
//
//trait TableWithRangeOps[V, PK, RK, F[_], S[_]] extends TableOps[V, (PK, RK), F, S] {
//  override val table: TableWithRangeKey[V, PK, RK]
//  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): S[DynamoFormat.Result[V]]
//}
