import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoDb
import akka.testkit.TestKit
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{ListTablesRequest, ListTablesResponse}

import scala.concurrent.Future

class Test extends TestKit(ActorSystem("Test"))
  with AnyFunSpecLike
  with Matchers
  with BeforeAndAfterAll
{
  private implicit val mat    = Materializer.matFromSystem

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val client: DynamoDbAsyncClient =
    LocalDynamoDB.clientConfig(DynamoDbAsyncClient.builder())
      .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
      .build()

  system.registerOnTermination(() -> client.close());

  val listTablesResult: Future[ListTablesResponse] =
    DynamoDb.single(ListTablesRequest.builder().build())

//  val source: SourceWithContext[PutItemRequest, SomeContext, NotUsed] = // ???
//
//  val flow: FlowWithContext[PutItemRequest, SomeContext, Try[PutItemResponse], SomeContext, NotUsed] =
//    DynamoDb.flowWithContext(parallelism = 1)
//
//  val writtenSource: SourceWithContext[PutItemResponse, SomeContext, NotUsed] = source
//    .via(flow)
//    .map {
//      case Success(response) => response
//      case Failure(exception) => throw exception
//    }
}
