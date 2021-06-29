package com.hiya.alternator.util

import akka.actor.ActorSystem
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import java.util
import java.util.concurrent.CompletableFuture
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.Random


class DynamoDbLossyClient(stableClient: DynamoDbAsyncClient)(implicit system: ActorSystem) extends DynamoDbAsyncClient {
    import system.dispatcher

    override def serviceName(): String = stableClient.serviceName()
    override def close(): Unit = stableClient.close()

    override def batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest): CompletableFuture[BatchWriteItemResponse] = {
      Random.nextDouble() match {
        case p if p < 0.2 =>
          Future.failed(ProvisionedThroughputExceededException.builder().build()).toJava.toCompletableFuture
        case p if p < 0.9 =>
          val (ok, fail) =
            fromItemMap(batchWriteItemRequest.requestItems())
              .partition(_ => Random.nextDouble() < 0.5)

          if (ok.isEmpty)
            Future.failed(ProvisionedThroughputExceededException.builder().build()).toJava.toCompletableFuture
          else {
            stableClient.batchWriteItem(
              BatchWriteItemRequest.builder().requestItems(toItemMap(ok)).build()
            ).toScala.map { resp =>
              BatchWriteItemResponse.builder()
                .unprocessedItems(toItemMap(fromItemMap(resp.unprocessedItems()) ++ fail))
                .build()
            }.toJava.toCompletableFuture
          }
        case _ =>
          stableClient.batchWriteItem(batchWriteItemRequest)
      }
    }

    private def toKeyMap(in: List[(String, java.util.Map[String, AttributeValue])]): java.util.Map[String, KeysAndAttributes] = {
      in.groupBy(_._1).map { case (table, keys) =>
        table -> KeysAndAttributes.builder().keys(keys.map(_._2).asJava).build()
      }.asJava
    }

    private def toItemMap(in: List[(String, WriteRequest)]): java.util.Map[String, java.util.List[WriteRequest]] = {
      in.groupBy(_._1).map { case (table, keys) =>
        table -> keys.map(_._2).asJava
      }.asJava
    }

    private def fromKeyMap(in: java.util.Map[String, KeysAndAttributes]): List[(String, util.Map[String, AttributeValue])] =
      in.asScala.toList.flatMap { case (table, keys) =>
        keys.keys().asScala.map(table -> _)
      }

    private def fromItemMap(in: java.util.Map[String, java.util.List[WriteRequest]]): List[(String, WriteRequest)] =
      in.asScala.toList.flatMap { case (table, req) =>
        req.asScala.map(table -> _)
      }

    override def batchGetItem(batchGetItemRequest: BatchGetItemRequest): CompletableFuture[BatchGetItemResponse] = {
      Random.nextDouble() match {
        case p if p < 0.2 =>
          Future.failed(ProvisionedThroughputExceededException.builder().build()).toJava.toCompletableFuture
        case p if p < 0.9 =>
          val (ok, fail) =
            fromKeyMap(batchGetItemRequest.requestItems())
              .partition(_ => Random.nextDouble() < 0.5)

          if (ok.isEmpty)
            Future.failed(ProvisionedThroughputExceededException.builder().build()).toJava.toCompletableFuture
          else {
            stableClient.batchGetItem(
              BatchGetItemRequest.builder().requestItems(toKeyMap(ok)).build()
            ).toScala.map { resp =>
              BatchGetItemResponse.builder()
                .responses(resp.responses())
                .unprocessedKeys(toKeyMap(fromKeyMap(resp.unprocessedKeys()) ++ fail))
                .build()
            }.toJava.toCompletableFuture
          }
        case _ =>
          stableClient.batchGetItem(batchGetItemRequest)
      }
    }

}
