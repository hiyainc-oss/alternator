package com.hiya.alternator

case class RetriesExhausted(lastError: Exception) extends
  Exception("Retries exhausted", lastError)
