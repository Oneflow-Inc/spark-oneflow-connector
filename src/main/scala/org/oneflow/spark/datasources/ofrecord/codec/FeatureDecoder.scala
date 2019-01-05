package org.oneflow.spark.datasources.ofrecord.codec

import oneflow.record._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

trait FeatureDecoder[T] {
  def decode(feature: Feature): T
}

abstract class ListFeatureDecoder[T](kind: Int) extends FeatureDecoder[Seq[T]] {
  def toSeq(feature: Feature): Seq[T]

  def decode(feature: Feature): Seq[T] = {
    require(feature != null)
    require(feature.kind.number == kind)
    toSeq(feature)
  }
}

class SingleFeatureDecoder[T](listFeatureDecoder: ListFeatureDecoder[T]) extends FeatureDecoder[T] {
  def decode(feature: Feature): T = {
    val list = listFeatureDecoder.decode(feature)
    require(list.size == 1)
    list.head
  }
}

object IntListFeatureDecoder extends ListFeatureDecoder[Int](Feature.INT32_LIST_FIELD_NUMBER) {
  override def toSeq(feature: Feature): Seq[Int] = feature.getInt32List.value
}

object IntFeatureDecoder extends SingleFeatureDecoder[Int](IntListFeatureDecoder)

object LongListFeatureDecoder extends ListFeatureDecoder[Long](Feature.INT32_LIST_FIELD_NUMBER) {
  override def toSeq(feature: Feature): Seq[Long] = feature.getInt32List.value.map {
    _.toLong
  }
}

object LongFeatureDecoder extends SingleFeatureDecoder[Long](LongListFeatureDecoder)

object FloatListFeatureDecoder extends ListFeatureDecoder[Float](Feature.FLOAT_LIST_FIELD_NUMBER) {
  override def toSeq(feature: Feature): Seq[Float] = feature.getFloatList.value
}

object FloatFeatureDecoder extends SingleFeatureDecoder[Float](FloatListFeatureDecoder)

object DoubleListFeatureDecoder
  extends ListFeatureDecoder[Double](Feature.DOUBLE_LIST_FIELD_NUMBER) {
  override def toSeq(feature: Feature): Seq[Double] = feature.getDoubleList.value
}

object DoubleFeatureDecoder extends SingleFeatureDecoder[Double](DoubleListFeatureDecoder)

object DecimalListFeatureDecoder
  extends ListFeatureDecoder[Decimal](Feature.FLOAT_LIST_FIELD_NUMBER) {
  override def toSeq(feature: Feature): Seq[Decimal] = feature.getFloatList.value.map {
    Decimal(_)
  }
}

object DecimalFeatureDecoder extends SingleFeatureDecoder[Decimal](DecimalListFeatureDecoder)

object BinaryListFeatureDecoder
  extends ListFeatureDecoder[Array[Byte]](Feature.BYTES_LIST_FIELD_NUMBER) {
  override def toSeq(feature: Feature): Seq[Array[Byte]] =
    feature.getBytesList.value.map(_.asScala.map {
      _.toByte
    }.toArray)
}

object BinaryFeatureDecoder extends SingleFeatureDecoder[Array[Byte]](BinaryListFeatureDecoder)

object StringListFeatureDecoder
  extends ListFeatureDecoder[UTF8String](Feature.BYTES_LIST_FIELD_NUMBER) {
  override def toSeq(feature: Feature): Seq[UTF8String] = feature.getBytesList.value.map { bs =>
    UTF8String.fromString(bs.toStringUtf8)
  }
}

object StringFeatureDecoder extends SingleFeatureDecoder[UTF8String](StringListFeatureDecoder)
