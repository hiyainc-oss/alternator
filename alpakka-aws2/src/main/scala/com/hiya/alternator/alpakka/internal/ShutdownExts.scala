package aws2.alpakka.internal

import com.hiya.alternator.aws2

//import akka.Done
//import akka.actor.typed.scaladsl.AskPattern._
//import akka.actor.typed.{ActorRef, Scheduler}
//import akka.util.Timeout
//import com.hiya.alternator.alpakka.{BatchedReadBehavior, BatchedWriteBehavior}
//
//import scala.concurrent.Future
//
//abstract class ShutdownExts[T] {
//  def terminate()(implicit timeout: Timeout, scheduler: Scheduler): Future[Done]
//}
//
//object ShutdownExts {
//  abstract class Support[T] {
//    def apply(actorRef: ActorRef[T]): ShutdownExts[T]
//  }
//
//  object Support {
//    implicit val writeSupport: Support[BatchedWriteBehavior.BatchedRequest] =
//      (actorRef: ActorRef[BatchedWriteBehavior.BatchedRequest]) =>
//        new ShutdownExts[BatchedWriteBehavior.BatchedRequest] {
//          override def terminate()(implicit timeout: Timeout, scheduler: Scheduler): Future[Done] = {
//            actorRef.ask(BatchedWriteBehavior.GracefulShutdown)
//          }
//        }
//
//    implicit val readSupport: Support[BatchedReadBehavior.BatchedRequest] =
//      (actorRef: ActorRef[BatchedReadBehavior.BatchedRequest]) =>
//        new ShutdownExts[BatchedReadBehavior.BatchedRequest] {
//          override def terminate()(implicit timeout: Timeout, scheduler: Scheduler): Future[Done] = {
//            actorRef.ask(BatchedReadBehavior.GracefulShutdown)
//          }
//        }
//  }
//}
