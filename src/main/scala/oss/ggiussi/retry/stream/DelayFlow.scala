package oss.ggiussi.retry.stream

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogicWithLogging}
import oss.ggiussi.retry.stream.RetriesStream.CommittableRetry

import scala.concurrent.duration.FiniteDuration

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
