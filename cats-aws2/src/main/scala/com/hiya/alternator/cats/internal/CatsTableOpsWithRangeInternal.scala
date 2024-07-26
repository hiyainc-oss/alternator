//package aws2.cats.internal
//
//import com.hiya.alternator.aws2
//
//class CatsTableOpsWithRangeInternal[F[_]: Async, V, PK, RK](
//  override val table: TableWithRangeKey[V, PK, RK],
//  override val client: Cats[F]
//) extends CatsTableOpsInternal[F, V, (PK, RK)](table, client)
//  with CatsTableOpsWithRange[F, V, PK, RK] {
//
//  override def query(pk: PK, rk: RKCondition[RK]): Stream[F, Result[V]] =
//    client.client
//      .queryPaginator(table.query(pk, rk).build())
//      .toStreamBuffered(1)
//      .flatMap(data => Stream.emits(table.deserialize(data)))
//}
