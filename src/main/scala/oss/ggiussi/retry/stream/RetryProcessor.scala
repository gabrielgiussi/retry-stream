package oss.ggiussi.retry.stream

import oss.ggiussi.retry.stream.RetryRecord.Payload

import scala.concurrent.Future

trait RetryProcessor {
  val PROCESSOR_TAG: String

  def process(payload: Payload): Future[Unit]

}
