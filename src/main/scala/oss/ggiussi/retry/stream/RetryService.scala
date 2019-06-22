package oss.ggiussi.retry.stream

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import oss.ggiussi.retry.stream.RetryRecord.Payload

import scala.concurrent.Future

trait RetryService {
  def retry(topic: String)(tag: String)(attempts: Int, payload: Payload): Future[Unit]

  def retry(tag: String, attempts: Int, payload: Payload): Future[Unit]
}

class RetryServiceImpl(bootstrapServers: String) extends RetryService {

  import scala.collection.JavaConverters._
  import Enhancements._

  val producer = new KafkaProducer[String, RetryRecord](Map[String, Object](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers).asJava, new StringSerializer, RetryRecordSerde)

  private def produce(topic: String, retry: RetryRecord): Future[Unit] = producer.xsend(new ProducerRecord(topic,retry))

  def retry(topic: String)(tag: String)(attempts: Int, payload: String): Future[Unit] = produce(topic,RetryRecord(tag, attempts, payload))

  def retry(tag: String, attempts: Int, payload: String) = retry(RetryTopics.TOPIC_1._1)(tag)(attempts, payload)
}

