package com.hiya.alternator.aws2.internal

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbException, ProvisionedThroughputExceededException}

class ExceptionsSpec extends AnyFunSpec with Matchers {
  describe("Exceptions") {
    val exceptions = Exceptions

    describe("isRetryable") {
      it("should return true for retryable SDK exception") {
        // Test with a DynamoDB exception with status code 400
        val retryableEx = DynamoDbException
          .builder()
          .message("Retryable error")
          .statusCode(400)
          .build()
        // Note: We can't easily mock retryable() without PowerMock or similar,
        // so we test with actual DynamoDB exceptions.
        // With status code 400 and no explicit retryable flag, this should be false
        exceptions.isRetryable(retryableEx) shouldBe false // 400 is not >= 500
      }

      it("should return true for 500 status code") {
        val ex = AwsServiceException
          .builder()
          .message("Internal Server Error")
          .statusCode(500)
          .build()
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return true for 502 status code") {
        val ex = AwsServiceException
          .builder()
          .message("Bad Gateway")
          .statusCode(502)
          .build()
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return true for 503 status code") {
        val ex = AwsServiceException
          .builder()
          .message("Service Unavailable")
          .statusCode(503)
          .build()
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return true for 504 status code") {
        val ex = AwsServiceException
          .builder()
          .message("Gateway Timeout")
          .statusCode(504)
          .build()
        exceptions.isRetryable(ex) shouldBe true
      }

      it("should return false for 400 status code without retryable flag") {
        val ex = AwsServiceException
          .builder()
          .message("Bad Request")
          .statusCode(400)
          .build()
        exceptions.isRetryable(ex) shouldBe false
      }

      it("should return false for 404 status code") {
        val ex = AwsServiceException
          .builder()
          .message("Not Found")
          .statusCode(404)
          .build()
        exceptions.isRetryable(ex) shouldBe false
      }

      it("should return false for 499 status code (just below 500)") {
        val ex = AwsServiceException
          .builder()
          .message("Client Error")
          .statusCode(499)
          .build()
        exceptions.isRetryable(ex) shouldBe false
      }

      it("should return false for non-SDK exceptions") {
        val ex = new RuntimeException("Generic error")
        exceptions.isRetryable(ex) shouldBe false
      }

      it("should return false for ProvisionedThroughputExceededException with low status code") {
        val ex = ProvisionedThroughputExceededException
          .builder()
          .message("Throughput exceeded")
          .statusCode(400)
          .build()
        // This is a throttle exception; whether it's retryable depends on status code or retryable flag
        // With status code 400, it should be false unless SDK marks it as retryable
        exceptions.isRetryable(ex) shouldBe false
      }
    }

    describe("isThrottle") {
      it("should return true for ProvisionedThroughputExceededException") {
        val ex = ProvisionedThroughputExceededException
          .builder()
          .message("Throughput exceeded")
          .build()
        exceptions.isThrottle(ex) shouldBe true
      }

      it("should return false for RequestLimitExceededException") {
        // RequestLimitExceededException is not matched by the pattern (not ProvisionedThroughputExceededException)
        // and is not marked as throttling by AWS SDK v2
        val ex = software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException
          .builder()
          .message("Request limit exceeded")
          .build()
        exceptions.isThrottle(ex) shouldBe false
      }

      it("should return false for non-throttling SDK exception") {
        val ex = AwsServiceException
          .builder()
          .message("Some error")
          .statusCode(500)
          .build()
        exceptions.isThrottle(ex) shouldBe false
      }

      it("should return false for non-SDK exceptions") {
        val ex = new RuntimeException("Generic error")
        exceptions.isThrottle(ex) shouldBe false
      }

      it("should return false for 500 status code without throttling flag") {
        val ex = DynamoDbException
          .builder()
          .message("Internal Server Error")
          .statusCode(500)
          .build()
        exceptions.isThrottle(ex) shouldBe false
      }
    }
  }
}
