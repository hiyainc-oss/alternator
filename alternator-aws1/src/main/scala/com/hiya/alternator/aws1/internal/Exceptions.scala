package com.hiya.alternator.aws1.internal

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.dynamodbv2.model.{
  LimitExceededException,
  ProvisionedThroughputExceededException,
  RequestLimitExceededException
}

class Exceptions {
  def isRetryable(ex: Throwable): Boolean = ex match {
    case ex: AmazonServiceException if ex.getStatusCode >= 500 => true
    case _: LimitExceededException => true
    case _: RequestLimitExceededException => true
    case _ => false
  }

  def isThrottle(ex: Throwable): Boolean = ex match {
    case _: ProvisionedThroughputExceededException => true
    case _ => false
  }
}

object Exceptions extends Exceptions
