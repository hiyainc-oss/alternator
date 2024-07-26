//package com.hiya.alternator
//
//import com.hiya.alternator.syntax.RKCondition
//import software.amazon.awssdk.services.dynamodb.model.{QueryRequest, QueryResponse}
//
//import scala.jdk.CollectionConverters._
//
//class TableWithRangeKey[V, PK, RK](tableName: String, schema: TableSchemaWithRange.Aux[V, PK, RK])
//  extends Table[V, (PK, RK)](tableName)(schema) {
//
//  def query(pk: PK, rk: RKCondition[RK] = RKCondition.empty): QueryRequest.Builder = {
//    rk.render(
//      schema.rkField,
//      RKCondition.EQ(pk)(schema.PK).render(schema.pkField, RKCondition.QueryBuilder)
//    )(
//      QueryRequest.builder().tableName(tableName)
//    )
//  }
//
//  final def deserialize(response: QueryResponse): List[DynamoFormat.Result[V]] = {
//    if (response.hasItems) response.items().asScala.toList.map(deserialize)
//    else Nil
//  }
//
//  override def withClient(client: Client): client.RKClient[V, PK, RK] =
//    client.createRkClient(this)
//}
