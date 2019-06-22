package oss.ggiussi.retry.stream

import java.util

import akka.actor.ActorRef
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ProducerMessage.Envelope
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Source}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._
import oss.ggiussi.retry.stream.RetryRecord.Payload

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object RetryRecordSerde extends Deserializer[RetryRecord] with Serializer[RetryRecord] {

  val (reader, writer) = {
    val mapper = SerializationUtils.mapper
    (mapper.readerFor(classOf[RetryRecord]), mapper.writerFor(classOf[RetryRecord]))
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  def read(data: Array[Byte]): RetryRecord = reader.readValue[RetryRecord](data)

  def write(record: RetryRecord): Array[Byte] = writer.writeValueAsBytes(record)

  override def deserialize(topic: String, data: Array[Byte]): RetryRecord = read(data)

  override def serialize(topic: String, data: RetryRecord): Array[Byte] = write(data)

  override def close(): Unit = ()

}

case class RetryMetadata(offset: ConsumerMessage.CommittableOffset, topic: String)

object RetryRecord {

  type Payload = String

  def apply(tag: String, retries: Int, payload: Payload): RetryRecord = RetryRecord.apply(tag, retries, System.currentTimeMillis(), payload)

  def apply(tag: String, retries: Int, timestamp: Long, payload: Payload): RetryRecord = new RetryRecord(tag, 1, retries, timestamp, payload)
}

case class RetryRecord(tag: String, attempt: Int, max: Int, timestamp: Long, payload: String) {
  protected[stream] def nextRetry: Option[RetryRecord] = if (attempt == max) None else Some(copy(attempt = attempt + 1, timestamp = System.currentTimeMillis()))
}

object RetriesStream {

  import Enhancements._

  type Processors = Map[String, Payload => Future[Unit]]

  type TopicSelector = (RetryRecord,String) => String

  object RetryResult {
    def failed(t: Throwable, r: RetryRecord): RetryResult = r.nextRetry.fold[RetryResult](Exhausted(t, r))(Failed(t, _))
  }

  sealed trait RetryResult

  case class Unprocessed(record: RetryRecord) extends RetryResult

  case class Succeed(record: RetryRecord) extends RetryResult

  case class Exhausted(t: Throwable, r: RetryRecord) extends RetryResult

  case class Failed(t: Throwable, next: RetryRecord) extends RetryResult

  case class RetryRecordDeserializationException(private val message: String = "",
                                                 private val cause: Throwable = None.orNull) extends RuntimeException(message, cause)

  def demultiplexer(processors: Processors) = (record: RetryRecord) =>
    processors.get(record.tag) match {
      case Some(p) => p(record.payload).fmap(_ => Succeed(record)).frecover { case t => RetryResult.failed(t, record) }
      case None => Future.successful(Unprocessed(record))
    }

  def demultiplexerFlow(processors: Processors, parallelism: Int) = {
    val f = demultiplexer(processors)
    Flow.apply[CommittableRetry].mapAsync(parallelism) { msg =>
      f(msg.record).fmap { result =>
        (result, msg.metadata)
      }
    }
  }

  def retryFunction(topicSelector: Option[TopicSelector]): ((RetryResult, RetryMetadata)) => Envelope[String, RetryRecord, ConsumerMessage.CommittableOffset] = {
    case (result, RetryMetadata(offset, topic)) => result match {
      case Failed(_, r) =>
        val nextTopic = topicSelector.fold(topic)(_.apply(r,topic))
        ProducerMessage.single(new ProducerRecord(nextTopic, r), offset)
      case _ => ProducerMessage.passThrough(offset)
    }
  }

  def retrySink(producerSettings: ProducerSettings[String, RetryRecord], topicSelector: Option[TopicSelector] = None) = Flow.fromFunction(retryFunction(topicSelector)).toMat(Producer.committableSink(producerSettings))(Keep.right)

  case class CommittableRetry(record: RetryRecord, metadata: RetryMetadata)

  def flowRetries[K, V](topics: Map[String, FiniteDuration], processors: Processors, f: V => RetryRecord) = {
    val resumeOnDeserializationException = ActorAttributes.withSupervisionStrategy {
      case _: RetryRecordDeserializationException =>
        Supervision.Resume
      case _: Throwable =>
        Supervision.stop
    }
    Flow.apply[(TopicPartition, Source[CommittableMessage[K, V], NotUsed])].flatMapMerge(Int.MaxValue, {
      case (topicPartition, source) => topics.get(topicPartition.topic()) match {
        case Some(delay) =>
          val (partition, topic) = (topicPartition.partition(), topicPartition.topic())
          source
            .via(Flow.fromFunction { case CommittableMessage(record, committableOffset) =>
              val retry = try {
                f(record.value)
              } catch {
                case e: Throwable => throw RetryRecordDeserializationException(cause = e)
              }
              CommittableRetry(retry, RetryMetadata(committableOffset, record.topic))
            }).withAttributes(resumeOnDeserializationException)
            .via(DelayFlow.retries(delay, Some(r => s" ${r.record.toString}  [partition = $partition, topic = $topic, topic delay = $delay]")))
            .via(demultiplexerFlow(processors, 1))
        case None => Source.empty
      }
    })
  }

  def source[V](topics: Set[String],
                consumerSettings: ConsumerSettings[String, V],
                rebalanceListener: Option[ActorRef] = None): Source[(TopicPartition, Source[CommittableMessage[String, V], NotUsed]), Consumer.Control] = {
    Consumer
      .committablePartitionedSource(consumerSettings, rebalanceListener.foldLeft(Subscriptions.topics(topics))(_.withRebalanceListener(_)))
  }

  def apply[V](topics: Map[String, FiniteDuration],
               cs: ConsumerSettings[String, V],
               ps: ProducerSettings[String, RetryRecord],
               processors: Processors,
               f: V => RetryRecord,
               rebalanceListener: Option[ActorRef] = None,
               topicSelector: Option[TopicSelector] = None
              ): RunnableGraph[(Consumer.Control, Future[Done])] = {
    val records = RetriesStream.source(topics.keySet, cs,rebalanceListener)
    val results = flowRetries[String,V](topics, processors, f)
    val producer = RetriesStream.retrySink(ps, topicSelector)
    records.via(results).toMat(producer)(Keep.both)
  }
}