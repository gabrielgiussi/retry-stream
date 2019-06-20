package oss.ggiussi.retry.stream

import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object Enhancements {

  private val SameThreadEC = new ExecutionContext {
    override def execute(runnable: Runnable): Unit = runnable.run()

    override def reportFailure(cause: Throwable): Unit = {
      throw cause
    }
  }

  implicit class EnhancedFuture[T](val future: Future[T]) extends AnyVal {
    def fmap[S](f: T => S) = future.map(f)(SameThreadEC)

    def fforeach[U](f: T => U) = future.foreach(f)(SameThreadEC)

    def fflatMap[S](f: T => Future[S]) = future.flatMap(f)(SameThreadEC)

    def fonComplete[U](f: Try[T] => U) = future.onComplete(f)(SameThreadEC)

    def frecover[U >: T](pf: PartialFunction[Throwable, U]) = future.recover(pf)(SameThreadEC)
  }

  implicit class EnhancedKafkaProducer[K, V](val producer: org.apache.kafka.clients.producer.KafkaProducer[K, V]) extends AnyVal {

    def xsend(record: ProducerRecord[K, V]): Future[Unit] = xsend(record, _ => ())

    // Warning!: f should be a lightweight computation
    def xsend[T](record: ProducerRecord[K, V], f: RecordMetadata => T): Future[T] = {
      val promise = Promise[T]()
      val callback = new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) promise.tryFailure(exception)
          else promise.trySuccess(f(metadata))
        }
      }
      producer.send(record, callback)
      promise.future
    }
  }
}


