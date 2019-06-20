package oss.ggiussi.retry.stream

import java.util

import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ProducerMessage.Envelope
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.stream.stage._
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._
import oss.ggiussi.retry.stream.RetriesStream.{CommittableRetry, Processors, TopicSelector}
import oss.ggiussi.retry.stream.RetryRecord.Payload
import Enhancements._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

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

object DelayFlow {

  import scala.concurrent.duration._

  def retries(delayInMillis: FiniteDuration, logIn: Option[CommittableRetry => String] = None): DelayFlow[CommittableRetry] = DelayFlow.apply(delayInMillis.toMillis, _.record.timestamp, logIn)

  def apply[IN](delayInMillis: Long, f: IN => Long, logIn: Option[IN => String] = None): DelayFlow[IN] = new DelayFlow(r => {
    val diff = System.currentTimeMillis() - (f(r) + delayInMillis)
    if (diff < 0) Some(diff.abs.millis)
    else None
  }, logIn)
}

class DelayFlow[IN](f: IN => Option[FiniteDuration], logIn: Option[IN => String] = None) extends GraphStage[FlowShape[IN, IN]] {
  val out: Outlet[IN] = Outlet("Retry.Out")
  val in: Inlet[IN] = Inlet("Retry.In")

  override def shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogicWithLogging(shape) with InHandler with OutHandler {

    override protected def onTimer(timerKey: Any): Unit = {
      push(out, timerKey.asInstanceOf[IN])
    }

    override def onPush(): Unit = {
      val record = grab(in)
      f(record) match {
        case Some(delay) =>
          log.debug(s"Delaying $delay ${logIn.map(_.apply(record)).getOrElse("")}")
          scheduleOnce(record, delay)
        case None =>
          push(out, record)
      }
    }

    override def onPull(): Unit = tryPull(in)

    setHandler(in, this)
    setHandler(out, this)
  }
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
        /* TODO
        result match {
          case Failed(t, _) => LOGGER.warn(s"A failure has occurred processing ${msg.record}. Sending to retries", t)
          case Unprocessed(_) => LOGGER.error(s"There is no processor for tag ${msg.record.tag}")
          case Exhausted(t, _) => LOGGER.error(s"Exhausted retries ${msg.record}", t)
          case _ => LOGGER.info(s"Succeed retry ${msg.record}")
        }
         */
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
      case e: RetryRecordDeserializationException =>
        Supervision.Resume
      case e: Throwable =>
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
    //maybeMetrics.foldLeft(records.via(results))(_ wireTap _).toMat(producer)(Keep.both)
  }
}

trait RetryService {
  def retry(topic: String)(tag: String)(attempts: Int, payload: Payload): Future[Unit]

  def retry(tag: String, attempts: Int, payload: Payload): Future[Unit]
}

trait RetryProcessor {
  val PROCESSOR_TAG: String

  def process(payload: Payload): Future[Unit]

}

object JsonRetryProcessor {
  def serializePayload(payload: Any): String = SerializationUtils.toJson(payload)
}

abstract class JsonRetryProcessor[A](implicit tag: ClassTag[A]) extends RetryProcessor {

  protected def process(payload: A): Future[Unit]

  override final def process(payload: Payload): Future[Unit] =  {
    Try(SerializationUtils.fromJson(payload, tag.runtimeClass.asInstanceOf[Class[A]])) match {
      case Success(event) =>
        process(event)
      case Failure(e) =>
        // RetriesStream.LOGGER.error(s"Error deserializing json in processor $PROCESSOR_TAG", e) TODO
        Future.unit
    }
  }

}

class RetryServiceImpl(bootstrapServers: String) extends RetryService {

  import scala.collection.JavaConverters._

  val producer = new KafkaProducer[String, RetryRecord](Map[String, Object](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers).asJava, new StringSerializer, RetryRecordSerde)

  private def produce(topic: String, retry: RetryRecord): Future[Unit] = producer.xsend(new ProducerRecord(topic,retry))

  def retry(topic: String)(tag: String)(attempts: Int, payload: String): Future[Unit] = produce(topic,RetryRecord(tag, attempts, payload))

  def retry(tag: String, attempts: Int, payload: String) = retry(RetriesTopics.TOPIC_1._1)(tag)(attempts, payload)
}

object RetryActor {

  import scala.concurrent.duration._

  def supervised(jumpAttempt: Int, topics: Map[String, FiniteDuration], processors: Processors, bootstrapServers: String, cs: ConsumerSettings[String, Array[Byte]], ps: ProducerSettings[String, RetryRecord])(implicit mat: ActorMaterializer) = BackoffSupervisor.props(
    Backoff.onFailure(
      Props(new RetryActor(jumpAttempt, topics, processors, bootstrapServers, cs, ps)),
      childName = "retries-stream",
      minBackoff = 15.seconds,
      maxBackoff = 5.minutes,
      randomFactor = 0.2,
      maxNrOfRetries = -1
    ).withDefaultStoppingStrategy.withSupervisorStrategy(OneForOneStrategy()({
      case e =>
        SupervisorStrategy.restart
    }))
  )
}

object RetriesTopics {
  import scala.concurrent.duration._

  val TOPIC_1 = ("retries1", 1.minute)

  val TOPIC_2 = ("retries2", 8.hours)

  val TOPICS = Map(TOPIC_1, TOPIC_2)
}

class RetryActor(jumpAttempt: Int, topics: Map[String, FiniteDuration], processors: Processors, bootstrapServers: String, consumerSettings: ConsumerSettings[String, Array[Byte]], producerSettings: ProducerSettings[String, RetryRecord])(implicit mat: ActorMaterializer) extends Actor with ActorLogging {

  val cs = consumerSettings.withBootstrapServers(bootstrapServers).withClientId("prometeo-retries")
  val ps = producerSettings.withBootstrapServers(bootstrapServers)
  val topicSelector: TopicSelector = {
    case (r,_) if r.attempt > jumpAttempt => RetriesTopics.TOPIC_2._1
    case (_,topic) => topic
  }

  var control: Option[DrainingControl[Done]] = None

  override def preStart(): Unit = {
    log.info(s"Starting RetryActor with { topics: [${topics.mkString(",")}] }")
    startStream()
    super.preStart()
  }

  def restartStream() = {
    log.warning(s"Restarting RetryActor with { topics: [${topics.mkString(",")}] }")
    stopStream()
    startStream()
  }

  def startStream() = {
    if (control.isEmpty) {
      val (c, done) = RetriesStream(topics, cs, ps, processors, RetryRecordSerde.read, topicSelector = Some(topicSelector)).run()(mat)
      done.failed.foreach(_ => self ! "restart")(context.dispatcher)
      control = Some(DrainingControl((c, done)))
    }
  }

  def stopStream() = {
    control.foreach(_.shutdown())
    control = None
  }

  override def postStop(): Unit = {
    stopStream()
    super.postStop()
  }

  override def receive: Receive = {
    case "stop" => stopStream()
    case "restart" => restartStream()
  }
}