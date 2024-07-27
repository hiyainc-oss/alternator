package com.hiya.alternator

import com.hiya.alternator.DynamoFormat.Result
import org.openjdk.jmh.annotations._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue


@State(Scope.Thread)
@Fork(3)
@Warmup(iterations = 5, batchSize = 1000, time = 1)
@Measurement(iterations = 3, batchSize = 1000, time = 1)
class AlternatorBenchmark {

  import FormatBenchmarkData._

  implicit val dynamoFormat: DynamoFormat[PS] = {
    import com.hiya.alternator.generic.auto._
    com.hiya.alternator.generic.semiauto.deriveCompound
  }

  val data10: PS = PS((0 until 10).map(genData).toList)
  val data100: PS = PS((0 until 100).map(genData).toList)
  val data1000: PS = PS((0 until 1000).map(genData).toList)

  val av10: AttributeValue = DynamoFormat[PS].write(data10)
  val av100: AttributeValue = DynamoFormat[PS].write(data100)
  val av1000: AttributeValue = DynamoFormat[PS].write(data1000)

  @Benchmark
  def write_10: AttributeValue = DynamoFormat[PS].write(data10)

  @Benchmark
  def write_100: AttributeValue = DynamoFormat[PS].write(data100)

  @Benchmark
  def write_1000: AttributeValue = DynamoFormat[PS].write(data1000)

  @Benchmark
  def read_10: Result[PS] = DynamoFormat[PS].read(av10)

  @Benchmark
  def read_100: Result[PS] = DynamoFormat[PS].read(av100)

  @Benchmark
  def read_1000: Result[PS] = DynamoFormat[PS].read(av1000)
}
