package com.hiya.alternator

import com.hiya.alternator.generic.semiauto
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import org.scanamo.generic.auto._
import org.scanamo.{DynamoFormat => ScanamoFormat}

object FormatBenchmark extends Bench.LocalTime {

  sealed trait P
  final case class P0(i: Int) extends P
  final case class P1(i: String) extends P
  final case class P2(p: P) extends P
  final case class P3(i: Option[Int]) extends P
  final case class P4(i: Float) extends P

  final case class PS(p: List[P])

  val sizes: Gen[Int] = Gen.single("size")(100000)

  def genData(i: Int): P = {
    i % 5 match {
      case 0 => P0(i)
      case 1 => P1(i.toString)
      case 2 => P2(genData(i + 7))
      case 3 if i % 2 == 0=> P3(Some(i))
      case 3 => P3(None)
      case 4 => P4(i.toFloat)
    }
  }

  val ranges: Gen[PS] = for {
    size <- sizes
  } yield {
    PS((0 until size).map(genData).toList)
  }

  performance of "DynamoFormat" in {
    implicit val dynamoFormat: DynamoFormat[PS] = {
      import com.hiya.alternator.generic.auto._
      semiauto.deriveCompound[PS]
    }

    (measure method "read").config(
      exec.requireGC -> true
    ) in {
      using(ranges) in { d =>
        DynamoFormat[PS].write(d)
      }
    }
  }

  performance of "ScanamoFormat" in {
    (measure method "read").config(
      exec.requireGC -> true
    ) in {
      using(ranges) in { d =>
        ScanamoFormat[PS].write(d).toAttributeValue
      }
    }
  }
}
