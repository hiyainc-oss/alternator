package com.hiya.alternator

object FormatBenchmarkData {

  sealed trait P
  final case class P0(i: Int) extends P
  final case class P1(i: String) extends P
  final case class P2(p: P) extends P
  final case class P3(i: Option[Int]) extends P
  final case class P4(i: Float) extends P

  final case class PS(p: List[P])

  def genData(i: Int): P = {
    i % 5 match {
      case 0 => P0(i)
      case 1 => P1(i.toString)
      case 2 => P2(genData(i + 7))
      case 3 if i % 2 == 0 => P3(Some(i))
      case 3 => P3(None)
      case 4 => P4(i.toFloat)
    }
  }
}
