package org.oneflow.spark.datasources.ofrecord.codec

import oneflow.record._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

object RowDecoder {
  def decode(record: OFRecord, schema: StructType): InternalRow = {
    InternalRow.fromSeq(schema.fields.map { field =>
      record.feature
        .get(field.name)
        .map {
          decodeFeature(_, field.dataType)
        }
        .getOrElse {
          if (field.nullable) None
          else throw new NullPointerException(s"Field ${field.name} does not allow null values")
        }
    })
  }

  def decodeFeature(feature: Feature, dataType: DataType): Any = {
    dataType match {
      case IntegerType => IntFeatureDecoder.decode(feature)
      case LongType => LongFeatureDecoder.decode(feature)
      case FloatType => FloatFeatureDecoder.decode(feature)
      case DoubleType => DoubleFeatureDecoder.decode(feature)
      case DecimalType() => DecimalFeatureDecoder.decode(feature)
      case StringType => StringFeatureDecoder.decode(feature)
      case BinaryType => BinaryFeatureDecoder.decode(feature)
      case ArrayType(IntegerType, _) => ArrayData.toArrayData(IntListFeatureDecoder.decode(feature))
      case ArrayType(LongType, _) => ArrayData.toArrayData(LongListFeatureDecoder.decode(feature))
      case ArrayType(FloatType, _) => ArrayData.toArrayData(FloatListFeatureDecoder.decode(feature))
      case ArrayType(DoubleType, _) =>
        ArrayData.toArrayData(DoubleListFeatureDecoder.decode(feature))
      case ArrayType(DecimalType(), _) =>
        ArrayData.toArrayData(DecimalListFeatureDecoder.decode(feature))
      case ArrayType(StringType, _) =>
        ArrayData.toArrayData(StringListFeatureDecoder.decode(feature))
      case ArrayType(BinaryType, _) =>
        ArrayData.toArrayData(BinaryListFeatureDecoder.decode(feature))
      case _ =>
        throw new scala.RuntimeException(
          s"Cannot convert Feature to unsupported data type $dataType")
    }
  }
}
