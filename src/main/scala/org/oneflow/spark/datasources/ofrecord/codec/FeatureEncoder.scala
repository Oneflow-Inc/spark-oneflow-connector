package org.oneflow.spark.datasources.ofrecord.codec

import com.google.protobuf.ByteString
import oneflow.record._

trait FeatureEncoder[T] {
  def encode(value: T): Feature
}

//object Int64ListFeatureEncoder extends FeatureEncoder[Seq[Long]] {
//  override def encode(value: Seq[Long]): Feature = {
//    require(value.forall({ v =>
//      v <= Int.MaxValue && v >= Int.MinValue
//    }))
//    Feature().withInt32List(Int32List(value.map {
//      _.toInt
//    }))
//  }
//}

object Int64ListFeatureEncoder extends FeatureEncoder[Seq[Long]] {
  override def encode(value: Seq[Long]): Feature = Feature().withInt64List(Int64List(value))
}

object Int32ListFeatureEncoder extends FeatureEncoder[Seq[Int]] {
  override def encode(value: Seq[Int]): Feature = Feature().withInt32List(Int32List(value))
}

object FloatListFeatureEncoder extends FeatureEncoder[Seq[Float]] {
  override def encode(value: Seq[Float]): Feature =
    Feature().withFloatList(FloatList(value))
}

object DoubleListFeatureEncoder extends FeatureEncoder[Seq[Double]] {
  override def encode(value: Seq[Double]): Feature =
    Feature().withDoubleList(DoubleList(value))
}

object BytesListFeatureEncoder extends FeatureEncoder[Seq[Array[Byte]]] {
  override def encode(value: Seq[Array[Byte]]): Feature =
    Feature().withBytesList(BytesList(value.map {
      ByteString.copyFrom
    }))
}
