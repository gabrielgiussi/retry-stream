package oss.ggiussi.retry.stream

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.testkit.scaladsl.{EmbeddedKafkaLike, ScalatestKafkaSpec}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions, TopicPartitionsRevoked}
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serializer, StringDeserializer, StringSerializer}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}
import oss.ggiussi.retry.stream.RetriesStream.{Succeed, TopicSelector, flowRetries}

import scala.concurrent.Await

abstract class KafkaSpecBase(kafkaPort: Int)
  extends ScalatestKafkaSpec(kafkaPort)
    with WordSpecLike
    with EmbeddedKafkaLike
    with Matchers
    with ScalaFutures
    with Eventually


trait RetryProducer {
  this: EmbeddedKafkaLike =>

  implicit def retryAsList(r: RetryRecord) = List(r)

  val serde = RetryRecordSerde

  val partition1 = 1

  val producerSettings = ProducerSettings(system, new StringSerializer, serde).withBootstrapServers(bootstrapServers)

  def produce[V](partition: Int)(topic: String)(v: List[V], ser: Serializer[V]) = awaitProduce(Source(v.map(new ProducerRecord[String, V](topic, partition, null, _))).runWith(Producer.plainSink(ProducerSettings(system, new StringSerializer, ser).withBootstrapServers(bootstrapServers))))

  def produceRetry(partition: Int)(topic: String)(records: List[RetryRecord]) = produce(partition)(topic)(records, serde)

  def produce0 = produceRetry(partition0) _

  def produce1 = produceRetry(partition1) _
}

class RetriesWithEmbeddedKafka extends KafkaSpecBase(9222) with RetryProducer with RetryRecords {
  override def createKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort, zooKeeperPort)

  import scala.concurrent.duration._

  val PARTITIONS = 2

  system.eventStream.setLogLevel(Logging.WarningLevel)

  def waitPartitionsAssigned(group: String, t: Duration = 10.seconds, topics: Int = 1) = waitUntilConsumerSummary(group) {
    case members => members.map(_.assignment().topicPartitions().size()).sum == PARTITIONS * topics
  }

  def consumerSettings(group: String): ConsumerSettings[String, Array[Byte]] = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
    .withGroupId(group)
    .withBootstrapServers(bootstrapServers)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def startMultiTopics(topics: Map[String,FiniteDuration],
                       cs: ConsumerSettings[String, Array[Byte]],
                       ps: ProducerSettings[String, RetryRecord] = producerSettings,
                       rebalancer: Option[ActorRef] = None,
                       topicSelector: Option[TopicSelector] = None) = {
    val processed = TestProbe()
    val processor = processors(processed.ref ! _)

    val topic = topics.head._1
    val partiton0 = produce0(topic)
    val partiton1 = produce1(topic)

    val control = RetriesStream(topics, cs, ps, processor, serde.read, rebalancer, topicSelector).mapMaterializedValue(DrainingControl.apply).run()

    (control, processed, partiton0, partiton1)
  }

  def startStream(topic: String,
                  cs: ConsumerSettings[String, Array[Byte]],
                  ps: ProducerSettings[String, RetryRecord] = producerSettings,
                  rebalancer: Option[ActorRef] = None,
                  delay: FiniteDuration = 1.day) = startMultiTopics(Map(topic -> delay),cs,ps,rebalancer,None)

  def createTopicAndStartStream[V](cs: ConsumerSettings[String, Array[Byte]],
                                   ps: ProducerSettings[String, RetryRecord] = producerSettings,
                                   rebalancer: Option[ActorRef] = None,
                                   delay: FiniteDuration = 1.day) = {
    val topic = createTopic(suffix = 0, partitions = PARTITIONS)
    val (_1,_2,_3,_4) = startStream(topic, cs, ps, rebalancer, delay)
    (_1,_2,_3,_4,topic)
  }

  "RetriesStream" should {
    "no rebalancing while partitions are delayed" in {
      val rebalanceListener = TestProbe()
      class RebalanceActor extends Actor {
        override def receive: Receive = {
          case TopicPartitionsRevoked(_, topics) if topics.nonEmpty => rebalanceListener.ref ! "Rebalance"
        }
      }
      val r = system.actorOf(Props(new RebalanceActor))

      val group = createGroupId()

      val cs = consumerSettings(group)
        .withProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 350.millis.toMillis.toString)
        .withPollInterval(200.millis)

      val (control, processed, partition_0, partition_1, _) = createTopicAndStartStream(cs, rebalancer = Some(r))

      partition_0(DELAYED.succeed("1"))
      partition_1(DELAYED.succeed("2"))

      waitPartitionsAssigned(group)

      processed.expectNoMessage()

      rebalanceListener.expectNoMessage()

      control.shutdown()
    }
    "continue processing from partition 0 when partition 1 is delayed" in {
      val group = createGroupId()

      val (control, processed, partition_0, partition_1,_) = createTopicAndStartStream(consumerSettings(group))

      partition_0(DATED.succeed("1"))
      partition_0(DATED.succeed("2"))

      waitPartitionsAssigned(group)

      processed.expectMsg("1")
      processed.expectMsg("2")

      partition_1(DELAYED.succeed("3"))

      processed.expectNoMessage()

      partition_0(DATED.succeed("4"))

      processed.expectMsg("4")

      control.shutdown()

    }
    "exhaust retries" in {
      val group = createGroupId()

      val (control, processed, partition_0, _, _) = createTopicAndStartStream(consumerSettings(group), delay = 100.millis)

      val payload = "1"
      val record = DATED.fail(payload)
      record.max should be > 1

      partition_0(record)

      waitPartitionsAssigned(group)

      processed.expectMsgAllOf(List.fill(record.max)(payload): _*)
      processed.expectNoMessage(3.seconds)

      control.shutdown()

    }
    "skip broken records" in {
      val group = createGroupId()

      val (control, processed, partition0, _, topic) = createTopicAndStartStream(consumerSettings(group))

      produce(0)(topic)(List("broken"), new StringSerializer)
      partition0(DATED.succeed("good"))

      waitPartitionsAssigned(group)

      processed.expectMsg("good")
      processed.expectNoMessage()

      control.shutdown()

    }
    "commit offsets when no record is produced" in {
      val group = createGroupId()
      val cs = consumerSettings(group)

      val (control, processed, partition0, _, topic) = createTopicAndStartStream(cs)

      partition0(DATED.succeed("1"))

      waitPartitionsAssigned(group)

      processed.expectMsg("1")
      processed.expectNoMessage()

      Await.ready(control.drainAndShutdown(), 1.minute)

      val (control2, processed2, _, _) = startStream(topic, cs)

      processed2.expectNoMessage()
      partition0(DATED.succeed("2"))

      waitPartitionsAssigned(group)

      processed2.expectMsg("2")
      processed2.expectNoMessage()

      control2.shutdown()
    }
    "send last attempt to another topic" in {
      val group = createGroupId()
      val probeGroup = createGroupId()
      val cs = consumerSettings(group)
      val topic1 = createTopic(suffix = 1, partitions = PARTITIONS)
      val topic2 = createTopic(suffix = 2, partitions = PARTITIONS)
      val topics = Map(topic1 -> 100.millis, topic2 -> 150.millis)
      val topicSelector: TopicSelector = (record,topic) => {
        // Send last attempt to topic 2
        if (record.attempt == MAX_RETRIES) topic2 else topic
      }

      val t = Consumer.plainSource(cs.withGroupId(probeGroup), Subscriptions.topics(topic1,topic2)).map(_.topic()).runWith(TestSink.probe)
      t.request(10)
      val (control, processed, partition0, _) = startMultiTopics(topics, cs, topicSelector = Some(topicSelector))

      partition0(DATED.fail("1"))

      waitPartitionsAssigned(group, topics = 2)

      t.expectNext(topic1,topic1,topic2)
      t.expectNoMessage(1.second)

      processed.expectMsgAllOf(List.fill(MAX_RETRIES)("1") : _*)
      processed.expectNoMessage()

      control.drainAndShutdown()
    }
  }

  "RetryService" must {
    def createService() = new RetryServiceImpl(bootstrapServers)

    "Send to retry topic" in {
      val topic = createTopic(suffix = 0, partitions = PARTITIONS)
      val group = createGroupId()
      val service = createService()
      Await.ready(service.retry(topic)(SUCCEED)(3, SerializationUtils.toJson(TestPayload("", Map("value" -> 1)))),5.seconds)
      val topics = Map(topic -> 1.second)

      val sink = RetriesStream.source(topics.keySet, consumerSettings(group)).via(flowRetries(topics, processors(), RetryRecordSerde.read)).map(_._1).runWith(TestSink.probe)

      sink.request(1)
      waitPartitionsAssigned(group, 20.seconds)

      sink.expectNextWithTimeoutPF(5.seconds, {
        case Succeed(RetryRecord(SUCCEED, 1, 3, _, payload)) if SerializationUtils.fromJson(payload,classOf[TestPayload]).field2.get("value").contains(1) =>
      })

    }
  }

}


