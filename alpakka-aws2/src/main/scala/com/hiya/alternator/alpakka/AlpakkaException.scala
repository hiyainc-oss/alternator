package aws2.alpakka

import com.hiya.alternator.aws2

sealed abstract class AlpakkaException(message: String, cause: Throwable) extends Exception(message, cause)

object AlpakkaException {
  final case class RetriesExhausted(lastError: Exception) extends AlpakkaException("Retries exhausted", lastError)

  case object Unprocessed extends AlpakkaException("Unprocessed entry", null)

}
