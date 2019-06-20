package oss.ggiussi.retry.stream

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.testkit.ConsumerResultFactory
import akka.kafka.testkit.scaladsl.{EmbeddedKafkaLike, ScalatestKafkaSpec}
import akka.stream._
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import oss.ggiussi.retry.stream.RetriesStream._
import oss.ggiussi.retry.stream.RetryRecord.Payload

import scala.concurrent.{Await, Future}

case class TestPayload(field1: String, field2: Map[String,Int])

trait RetryRecords {

  val MAX_RETRIES = 3

  val FAIL = "fail"
  val SUCCEED = "succeed"
  val UNDEFINED = "undefined"

  def processors(f: Any => Unit = _ => ()): Processors = Map(
    FAIL -> (p => {
      f(p)
      Future.failed(new RuntimeException)
    }),
    SUCCEED -> (p => {
      f(p)
      Future.unit
    })
  )

  class RetryFactory(timestamp: Unit => Long) {
    private def retry(tag: String, payload: Payload, retries: Int = MAX_RETRIES) = RetryRecord(tag, retries, timestamp(()), payload)

    def exhausted(p: Payload) = retry(FAIL, p, 1)

    def fail(p: Payload) = retry(FAIL, p)

    def succeed(p: Payload) = retry(SUCCEED, p)

    def unprocessed(p: Payload) = retry(UNDEFINED, p)

  }

  val DELAYED = new RetryFactory(_ => System.currentTimeMillis())

  val DATED = new RetryFactory(_ => Instant.now().minus(300, ChronoUnit.DAYS).toEpochMilli)
}

class RetriesStreamSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with RetryRecords {

  import scala.concurrent.duration._

  implicit val mat = ActorMaterializer()

  def this() = this(ActorSystem(
    "RetriesSourceActorSpecSystem",
    ConfigFactory.parseString(
      """
      akka {
        loggers = ["akka.testkit.TestEventListener"]
        loglevel = "OFF"
      }
      """)))

  override protected def afterAll(): Unit = {
    mat.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  trait MockKafka {

    type Message = ConsumerMessage.CommittableMessage[String, RetryRecord]
    type E = (TopicPartition, Source[Message, NotUsed])

    val group = "test-group"
    val topic = "test-topic"

    def retryRecordMessage(topic: String)(partition: Int)(offset: Int)(r: RetryRecord): Message = {
      val off = ConsumerResultFactory.committableOffset(group, topic, partition, offset, "")
      val record = new ConsumerRecord[String, RetryRecord](topic, partition, offset, null, r)
      ConsumerResultFactory.committableMessage(record, off)
    }

    val PARTITION_0 = retryRecordMessage(topic)(0) _

    val PARTITION_1 = retryRecordMessage(topic)(1) _

  }

  "RetryRecordSerde" must {
    val serde = RetryRecordSerde
    "serialize and deserialize a RetryRecord" in {
      val r = RetryRecord("some tag", 3, 123456, SerializationUtils.toJson(TestPayload("field1", Map("a" -> 123))))
      serde.read(serde.write(r)) shouldBe r
    }
    "deserialize a json and get a RetryRecord" in {
      val r =
        """
          | {
          |   "tag": "tag1",
          |   "attempt": 2,
          |   "max": 3,
          |   "timestamp": 123456,
          |   "payload": "{ \"v1\": \"k1\", \"v2\": \"k2\" }"
          | }
        """.stripMargin
      serde.read(r.getBytes) shouldBe RetryRecord("tag1", 2, 3, 123456, """{ "v1": "k1", "v2": "k2" }""")
    }
  }

  "JsonRetryProcessor" must {
    "read json as defined type" in {
      var processed: TestPayload = null
      val p = new JsonRetryProcessor[TestPayload]() {
        override val PROCESSOR_TAG: String = ""

        override protected def process(payload: TestPayload): Future[Unit] = {
          processed = payload
          Future.unit
        }
      }
      val payload = TestPayload("abc", Map("a" -> 123))
      Await.ready(p.process(JsonRetryProcessor.serializePayload(payload)),1.minute)
      processed shouldBe payload
    }
  }

  "DelayFlow" must {
    "delay pushing downstream" in {
      val (source, sink) = TestSource.probe[Int]
        .via(new DelayFlow(i => Some((i * 100).millis)))
        .toMat(TestSink.probe)(Keep.both)
        .run

      sink.request(2)
      source.sendNext(3)

      within(300.millis, 600.millis) {
        sink.expectNext(3)
      }

      source.sendNext(2)
      within(200.millis, 400.millis) {
        sink.expectNext(2)
      }
    }
  }

  "Retries Flow" must {
    val processor = RetriesStream.demultiplexer(processors())

    def process(payload: RetryRecord) = Await.result(processor.apply(payload), 1.second)

    "return Success if record was processed" in {
      val r = DATED.succeed("1")
      process(r) shouldBe Succeed(r)
    }
    "return Unprocessed if the tag is not defined" in {
      val r = DATED.unprocessed("1")
      process(r) shouldBe Unprocessed(r)
    }
    "return Failure if the record wasn't processed correctly" in {
      val r = DATED.fail("1")
      process(r) should matchPattern { case Failed(_, _) => }
    }
    "retry if max attempts was not reached" in {
      val r = DATED.fail("1")
      process(r) should matchPattern { case Failed(_, RetryRecord(r.tag, a, r.max, t, r.payload)) if t > r.timestamp && r.attempt + 1 == a => }
    }
    "not retry if max attempts was reached" in {
      val r = DATED.exhausted("1")
      process(r) should matchPattern { case Exhausted(_, _) => }
    }
    "continue polling from partition 1 when partition 0 is delayed" in new MockKafka {
      val (source, sink) = TestSource.probe[E]
        .via(RetriesStream.flowRetries(Map(topic -> 1.day), processors(), identity))
        .map {
          case (Succeed(r), _) => r.payload
          case _ =>
        }
        .toMat(TestSink.probe[Any])(Keep.both)
        .run

      val delayedPartition = (
        new TopicPartition(topic, 0),
        Source(1 to 20).map(PARTITION_0(_)(DELAYED.succeed("A")))
      )
      val nonEmptyPartition = (
        new TopicPartition(topic, 1),
        Source(1 to 20).map(PARTITION_1(_)(DATED.succeed("B")))
      )

      sink.request(40)
      source.sendNext(delayedPartition)
      source.sendNext(nonEmptyPartition)

      sink.expectNextUnorderedN(List.fill(20)("B"))
      sink.expectNoMessage(100.millis)

    }

  }


}