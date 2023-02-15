package com.hiya.alternator.aws2


import com.hiya.alternator.FormatBenchmarkData
import org.openjdk.jmh.annotations._
import org.scanamo._
import org.scanamo.generic.semiauto._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue



@State(Scope.Thread)
@Fork(3)
@Warmup(iterations = 5, batchSize = 1000, time = 1)
@Measurement(iterations = 3, batchSize = 1000, time = 1)
class Aws2Benchmark {
  import FormatBenchmarkData._

  implicit val pFormat: DynamoFormat[P] = {
    import org.scanamo.generic.auto._
    deriveDynamoFormat[P]
  }

  implicit val psFormat: DynamoFormat[PS] = {
    deriveDynamoFormat[PS]
  }

  val data1: PS = PS((0 until 1).map(genData).toList)
  val data10: PS = PS((0 until 10).map(genData).toList)
  val data100: PS = PS((0 until 100).map(genData).toList)
  val data1000: PS = PS((0 until 1000).map(genData).toList)
  val data10000: PS = PS((0 until 10000).map(genData).toList)

  val av1: AttributeValue = DynamoFormat[PS].write(data1).toAttributeValue
  val av10: AttributeValue = DynamoFormat[PS].write(data10).toAttributeValue
  val av100: AttributeValue = DynamoFormat[PS].write(data100).toAttributeValue
  val av1000: AttributeValue = DynamoFormat[PS].write(data1000).toAttributeValue
  val av10000: AttributeValue = DynamoFormat[PS].write(data10000).toAttributeValue

  @Benchmark
  def write_1: AttributeValue = DynamoFormat[PS].write(data1).toAttributeValue

  @Benchmark
  def write_10: AttributeValue = DynamoFormat[PS].write(data10).toAttributeValue

  @Benchmark
  def write_100: AttributeValue = DynamoFormat[PS].write(data100).toAttributeValue

  @Benchmark
  def write_1000: AttributeValue = DynamoFormat[PS].write(data1000).toAttributeValue

  @Benchmark
  def write_10000: AttributeValue = DynamoFormat[PS].write(data10000).toAttributeValue

//  @Benchmark
  def read_1: Either[DynamoReadError, PS] = DynamoFormat[PS].read(av1)

  @Benchmark
  def read_10: Either[DynamoReadError, PS] = DynamoFormat[PS].read(av10)

  @Benchmark
  def read_100: Either[DynamoReadError, PS] = DynamoFormat[PS].read(av100)

  @Benchmark
  def read_1000: Either[DynamoReadError, PS] = DynamoFormat[PS].read(av1000)

  @Benchmark
  def read_10000: Either[DynamoReadError, PS] = DynamoFormat[PS].read(av10000)
}
