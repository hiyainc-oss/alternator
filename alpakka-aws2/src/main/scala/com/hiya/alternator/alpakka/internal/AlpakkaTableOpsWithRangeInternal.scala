//package aws2.alpakka.internal
//
//import com.hiya.alternator.aws2
//
//class AlpakkaTableOpsWithRangeInternal[V, PK, RK](override val table: TableWithRangeKey[V, PK, RK], client: Alpakka)(
//  implicit system: ClassicActorSystemProvider
//) extends AlpakkaTableOpsInternal[V, (PK, RK)](table, client)
//  with AlpakkaTableOpsWithRange[V, PK, RK] {
//
//  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): Source[DynamoFormat.Result[V], NotUsed] = {
//    val q = table.query(pk, rk).build()
//    DynamoDb.source(q).mapConcat(table.deserialize)
//  }
//
//}
