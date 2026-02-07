package com.hiya.alternator.aws1.internal

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.dynamodbv2.model.{
  LimitExceededException,
  ProvisionedThroughputExceededException,
  RequestLimitExceededException
}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ExceptionsSpec extends AnyFunSpec with Matchers {
  describe("Exceptions") {
    val exceptions = Exceptions

    describe("isRetryable") {
      it("should return true for 500 status code") {
        val ex = new AmazonServiceException("Internal Server Error")
        ex.setStatusCode(500)
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return true for 502 status code") {
        val ex = new AmazonServiceException("Bad Gateway")
        ex.setStatusCode(502)
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return true for 503 status code") {
        val ex = new AmazonServiceException("Service Unavailable")
        ex.setStatusCode(503)
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return true for 504 status code") {
        val ex = new AmazonServiceException("Gateway Timeout")
        ex.setStatusCode(504)
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return true for LimitExceededException") {
        val ex = new LimitExceededException("Limit exceeded")
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return true for RequestLimitExceededException") {
        val ex = new RequestLimitExceededException("Request limit exceeded")
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return false for 400 status code") {
        val ex = new AmazonServiceException("Bad Request")
        ex.setStatusCode(400)
        exceptions.isRetryable(ex) shouldBe false
      }

      it("should return false for 404 status code") {
        val ex = new AmazonServiceException("Not Found")
        ex.setStatusCode(404)
        exceptions.isRetryable(ex) shouldBe false
      }

      it("should return false for 499 status code (just below 500)") {
        val ex = new AmazonServiceException("Client Error")
        ex.setStatusCode(499)
        exceptions.isRetryable(ex) shouldBe false
      }

      it("should return false for non-AWS exceptions") {
        val ex = new RuntimeException("Generic error")
        exceptions.isRetryable(ex) shouldBe false
      }

      it("should return false for ProvisionedThroughputExceededException") {
        // This is a throttle exception, not a retryable error
        val ex = new ProvisionedThroughputExceededException("Throughput exceeded")
        exceptions.isRetryable(ex) shouldBe false
      }
    }

    describe("isThrottle") {
      it("should return true for ProvisionedThroughputExceededException") {
        val ex = new ProvisionedThroughputExceededException("Throughput exceeded")
        exceptions.isThrottle(ex) shouldBe true
      }

      it("should return false for RequestLimitExceededException") {
        // RequestLimitExceededException is a retryable error, not a throttle
        val ex = new RequestLimitExceededException("Request limit exceeded")
        exceptions.isThrottle(ex) shouldBe false
      }

      it("should return false for LimitExceededException") {
        val ex = new LimitExceededException("Limit exceeded")
        exceptions.isThrottle(ex) shouldBe false
      }

      it("should return false for 500 status code") {
        val ex = new AmazonServiceException("Internal Server Error")
        ex.setStatusCode(500)
        exceptions.isThrottle(ex) shouldBe false
      }

      it("should return false for non-AWS exceptions") {
        val ex = new RuntimeException("Generic error")
        exceptions.isThrottle(ex) shouldBe false
      }
    }
  }
}
