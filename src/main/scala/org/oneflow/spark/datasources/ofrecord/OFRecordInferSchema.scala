package org.oneflow.spark.datasources.ofrecord

import oneflow.record.{Feature, OFRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

object OFRecordInferSchema {

  def apply[T: TypeTag](rdd: RDD[T]): StructType = {
    typeOf[T] match {
      case t if t =:= typeOf[Array[Byte]] =>
        StructType(
          rdd
            .asInstanceOf[RDD[Array[Byte]]]
            .flatMap { bs =>
              OFRecord.parseFrom(bs).feature.mapValues(inferField)
            }
            .reduceByKey {
              case (a, b) if a == b =>
                a
              case (BinaryType | StringType, BinaryType | StringType) =>
                BinaryType
              case (
                dt@ArrayType(BinaryType | StringType, _),
                ArrayType(BinaryType | StringType, _)) =>
                dt.copy(elementType = BinaryType)
              case (dt@ArrayType(a@(BinaryType | StringType), _), b@(BinaryType | StringType))
                if a == BinaryType || b == BinaryType =>
                dt.copy(elementType = BinaryType)
              case (a@(BinaryType | StringType), dt@ArrayType(b@(BinaryType | StringType), _))
                if a == BinaryType || b == BinaryType =>
                dt.copy(elementType = BinaryType)
              case (
                dt@ArrayType(a, _),
                b@(IntegerType | FloatType | DoubleType | BinaryType | StringType)) if a == b =>
                dt
              case (
                a@(IntegerType | FloatType | DoubleType | BinaryType | StringType),
                dt@ArrayType(b, _)) if a == b =>
                dt
              case _ =>
                throw new RuntimeException()
            }
            .collect()
            .sortBy(_._1)
            .map {
              case (name, dt) => StructField(name, dt)
            })
      case _ =>
        throw new IllegalArgumentException(s"Unsupported recordType: recordType can be OFRecord")
    }
  }

  private def inferField(feature: Feature): DataType = {

    def isPrintable(b: Byte): Boolean = {
      b >= 32 && b <= 127
    }

    (feature.kind.number match {
      case Feature.BYTES_LIST_FIELD_NUMBER =>
        if (feature.getBytesList.value.forall {
          _.toByteArray.forall(isPrintable)
        }) {
          (StringType, feature.getBytesList.value.size)
        } else {
          (BinaryType, feature.getBytesList.value.size)
        }
      case Feature.INT32_LIST_FIELD_NUMBER =>
        (IntegerType, feature.getInt32List.value.size)
      case Feature.INT64_LIST_FIELD_NUMBER =>
        (LongType, feature.getInt64List.value.size)
      case Feature.FLOAT_LIST_FIELD_NUMBER =>
        (FloatType, feature.getFloatList.value.size)
      case Feature.DOUBLE_LIST_FIELD_NUMBER =>
        (DoubleType, feature.getDoubleList.value.size)
      case _ =>
        throw new RuntimeException("unsupported type ...")
    }) match {
      case (dt, 1) => dt
      case (dt, _) => ArrayType(dt)
    }
  }
}
