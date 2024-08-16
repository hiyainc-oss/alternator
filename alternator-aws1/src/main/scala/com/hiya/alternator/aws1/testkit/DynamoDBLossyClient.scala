package com.hiya.alternator.aws1.testkit

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{AbstractAmazonDynamoDBAsync, AmazonDynamoDBAsync}

import java.util
import java.util.concurrent.{CompletableFuture, Future}
import scala.jdk.CollectionConverters._
import scala.util.Random


class DynamoDBLossyClient(stableClient: AmazonDynamoDBAsync) extends AbstractAmazonDynamoDBAsync {

  override def batchWriteItemAsync(batchWriteItemRequest: BatchWriteItemRequest, asyncHandler: AsyncHandler[BatchWriteItemRequest, BatchWriteItemResult]): Future[BatchWriteItemResult] = {
    def failed(ex: Exception): Future[BatchWriteItemResult] = {
      if (asyncHandler != null) {
        asyncHandler.onError(ex)
      }

      CompletableFuture.failedFuture(ex)
    }

    Random.nextDouble() match {
      case p if p < 0.2 =>
        failed(new ProvisionedThroughputExceededException("Failed"))
      case p if p < 0.9 =>
        val (ok, fail) =
          fromItemMap(batchWriteItemRequest.getRequestItems)
            .partition(_ => Random.nextDouble() < 0.5)

        if (ok.isEmpty)
          failed(new ProvisionedThroughputExceededException("Failed"))
        else {
          val promise = new CompletableFuture[BatchWriteItemResult]()

          def complete(resp: BatchWriteItemResult): Unit = {
            if (asyncHandler != null)
              asyncHandler.onSuccess(batchWriteItemRequest, resp)
          }

          def failed(ex: Exception): Unit = {
            if (asyncHandler != null)
              asyncHandler.onError(ex)
            val _ = promise.completeExceptionally(ex)
          }

          stableClient.batchWriteItemAsync(
            new BatchWriteItemRequest(toItemMap(ok)),
            new AsyncHandler[BatchWriteItemRequest, BatchWriteItemResult] {
              override def onError(exception: Exception): Unit =
                failed(exception)
              override def onSuccess(request: BatchWriteItemRequest, result: BatchWriteItemResult): Unit =
                complete(
                  new BatchWriteItemResult()
                    .withUnprocessedItems(toItemMap(fromItemMap(result.getUnprocessedItems) ++ fail))
                )
            }
          )

          promise
        }
      case _ =>
        stableClient.batchWriteItemAsync(batchWriteItemRequest, asyncHandler)
    }
  }

  private def toKeyMap(in: List[(String, java.util.Map[String, AttributeValue])]): java.util.Map[String, KeysAndAttributes] = {
    in.groupBy(_._1).map { case (table, keys) =>
      table -> new KeysAndAttributes().withKeys(keys.map(_._2).asJava)
    }.asJava
  }

  private def toItemMap(in: List[(String, WriteRequest)]): java.util.Map[String, java.util.List[WriteRequest]] = {
    in.groupBy(_._1).map { case (table, keys) =>
      table -> keys.map(_._2).asJava
    }.asJava
  }

  private def fromKeyMap(in: java.util.Map[String, KeysAndAttributes]): List[(String, util.Map[String, AttributeValue])] =
    in.asScala.toList.flatMap { case (table, keys) =>
      keys.getKeys.asScala.map(table -> _)
    }

  private def fromItemMap(in: java.util.Map[String, java.util.List[WriteRequest]]): List[(String, WriteRequest)] =
    in.asScala.toList.flatMap { case (table, req) =>
      req.asScala.map(table -> _)
    }

  override def batchGetItemAsync(batchGetItemRequest: BatchGetItemRequest, asyncHandler: AsyncHandler[BatchGetItemRequest, BatchGetItemResult]): Future[BatchGetItemResult] = {
    def failed(ex: Exception): Future[BatchGetItemResult] = {
      if (asyncHandler != null)
        asyncHandler.onError(ex)

      CompletableFuture.failedFuture(ex)
    }

    Random.nextDouble() match {
      case p if p < 0.2 =>
        failed(new ProvisionedThroughputExceededException("failed"))
      case p if p < 0.9 =>
        val (ok, fail) =
          fromKeyMap(batchGetItemRequest.getRequestItems)
            .partition(_ => Random.nextDouble() < 0.5)

        if (ok.isEmpty)
          failed(new ProvisionedThroughputExceededException("failed"))
        else {
          val promise = new CompletableFuture[BatchGetItemResult]()

          def complete(resp: BatchGetItemResult): Unit = {
            if (asyncHandler != null)
              asyncHandler.onSuccess(batchGetItemRequest, resp)
            val _ = promise.complete(resp)
          }

          def failed(ex: Exception): Unit = {
            if (asyncHandler != null)
              asyncHandler.onError(ex)
            val _ = promise.completeExceptionally(ex)
          }

          stableClient.batchGetItemAsync(
            new BatchGetItemRequest(toKeyMap(ok)),
            new AsyncHandler[BatchGetItemRequest, BatchGetItemResult] {
              override def onError(exception: Exception): Unit =
                  failed(exception)
              override def onSuccess(request: BatchGetItemRequest, result: BatchGetItemResult): Unit =
                  complete(
                  new BatchGetItemResult()
                      .withResponses(result.getResponses)
                      .withUnprocessedKeys(toKeyMap(fromKeyMap(result.getUnprocessedKeys) ++ fail))
                  )
            }
          )

          promise
        }
      case _ =>
        stableClient.batchGetItemAsync(batchGetItemRequest, asyncHandler)
    }
  }

}
