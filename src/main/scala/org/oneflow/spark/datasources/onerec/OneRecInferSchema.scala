package org.oneflow.spark.datasources.onerec

import java.nio.ByteBuffer

import onerec.{Example, TensorData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

object OneRecInferSchema {

  def apply[T: TypeTag](rdd: RDD[T]): StructType = {
    typeOf[T] match {
      case t if t =:= typeOf[Array[Byte]] =>
        StructType(
          rdd
            .asInstanceOf[RDD[Array[Byte]]]
            .flatMap { bs =>
              val example = Example.getRootAsExample(ByteBuffer.wrap(bs))
              0.until(example.featuresLength()).map { example.features }.flatMap {
                feature =>
                  Option(feature.tensor()).map {
                    tensor =>
                      feature.name() -> (tensor.dataType() match {
                        case TensorData.Int8List =>
                          ArrayType(ByteType)
                        case TensorData.Int16List =>
                          ArrayType(ShortType)
                        case TensorData.Int32List =>
                          ArrayType(IntegerType)
                        case TensorData.Int64List =>
                          ArrayType(LongType)
                        case TensorData.Float32List =>
                          ArrayType(FloatType)
                        case TensorData.Float64List =>
                          ArrayType(DoubleType)
                        case TensorData.StringList =>
                          ArrayType(StringType)
                        case TensorData.BinaryList =>
                          ArrayType(BinaryType)
                      })
                  }
              }
            }
            .reduceByKey {
              case (a, b) if a == b =>
                a
              case _ =>
                throw new RuntimeException()
            }
            .collect()
            .sortBy(_._1)
            .map {
              case (name, dt) => StructField(name, dt)
            })
      case _ =>
        throw new IllegalArgumentException(s"Unsupported recordType: recordType can be OneRec")
    }
  }

}
