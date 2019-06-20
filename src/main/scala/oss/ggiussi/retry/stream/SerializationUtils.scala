package oss.ggiussi.retry.stream

import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object SerializationUtils {

  // FIXME
  val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
    .setSerializationInclusion(NON_ABSENT)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

  def toJson[T](obj: T) = mapper.writeValueAsString(obj)
  def fromJson[A](data: String, clazz: Class[A]): A = mapper.readValue(data, clazz)
  def fromJsonWithType[A](typeRef: TypeReference[A])(data: String): A = mapper.readValue(data, typeRef)

}
