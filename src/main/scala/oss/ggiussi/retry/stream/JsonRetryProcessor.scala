package oss.ggiussi.retry.stream

import oss.ggiussi.retry.stream.RetryRecord.Payload

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

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
        Future.unit
    }
  }

}
