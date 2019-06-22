package oss.ggiussi.retry.stream

import akka.Done
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.stream.ActorMaterializer
import oss.ggiussi.retry.stream.RetriesStream.{Processors, TopicSelector}

import scala.concurrent.duration.FiniteDuration

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



class RetryActor(jumpAttempt: Int, topics: Map[String, FiniteDuration], processors: Processors, bootstrapServers: String, consumerSettings: ConsumerSettings[String, Array[Byte]], producerSettings: ProducerSettings[String, RetryRecord])(implicit mat: ActorMaterializer) extends Actor with ActorLogging {

  val cs = consumerSettings.withBootstrapServers(bootstrapServers).withClientId("prometeo-retries")
  val ps = producerSettings.withBootstrapServers(bootstrapServers)
  val topicSelector: TopicSelector = {
    case (r,_) if r.attempt > jumpAttempt => RetryTopics.TOPIC_2._1
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
