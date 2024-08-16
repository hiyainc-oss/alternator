package com.hiya.alternator

sealed abstract class BatchedException(message: String, cause: Throwable) extends Exception(message, cause)

object BatchedException {
  final case class RetriesExhausted(lastError: Throwable) extends BatchedException("Retries exhausted", lastError)

  case object Unprocessed extends BatchedException("Unprocessed entry", null)

}
