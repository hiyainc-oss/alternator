package com.hiya.alternator.aws2.internal

import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException

class Exceptions {
  def isRetryable(ex: Throwable): Boolean = ex match {
    case ex: SdkServiceException if ex.retryable() || ex.statusCode >= 500 => true
    case _ => false
  }

  def isThrottle(ex: Throwable): Boolean = ex match {
    case _ : ProvisionedThroughputExceededException => true
    case ex : SdkServiceException if ex.isThrottlingException => true
    case _ => false
  }
}
