package oss.ggiussi.retry.stream

object RetryTopics {
  import scala.concurrent.duration._

  val TOPIC_1 = ("retries1", 1.minute)

  val TOPIC_2 = ("retries2", 8.hours)

  val TOPICS = Map(TOPIC_1, TOPIC_2)
}
