package org.oneflow.spark.datasources.ofrecord.codec

import oneflow.record._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object RowEncoder {

  def encode(row: InternalRow, schema: StructType): OFRecord = {
    OFRecord().withFeature(schema.zipWithIndex.flatMap {
      case (structField, index) if !row.isNullAt(index) =>
        Some(structField.name -> encodeFeature(row, structField, index))
      case (structField, _) if structField.nullable =>
        None
      case (structField, _) =>
        throw new NullPointerException(s"${structField.name} does not allow null values")
    }.toMap)
  }

  private def encodeFeature(row: InternalRow, structField: StructField, index: Int): Feature = {
    structField.dataType match {
      case IntegerType => Int32ListFeatureEncoder.encode(Seq(row.getInt(index)))
      case LongType => Int64ListFeatureEncoder.encode(Seq(row.getLong(index)))
      case FloatType => FloatListFeatureEncoder.encode(Seq(row.getFloat(index)))
      case DoubleType => DoubleListFeatureEncoder.encode(Seq(row.getDouble(index)))
      case DecimalType() => FloatListFeatureEncoder.encode(Seq(row.getDouble(index).toFloat))
      case StringType =>
        BytesListFeatureEncoder.encode(Seq(row.getUTF8String(index).getBytes))
      case BinaryType => BytesListFeatureEncoder.encode(Seq(row.getBinary(index)))
      case ArrayType(IntegerType, _) =>
        Int32ListFeatureEncoder.encode(row.getArray(index).toIntArray())
      case ArrayType(LongType, _) =>
        Int64ListFeatureEncoder.encode(row.getArray(index).toLongArray())
      case ArrayType(FloatType, _) =>
        FloatListFeatureEncoder.encode(row.getArray(index).toFloatArray())
      case ArrayType(DoubleType, _) =>
        DoubleListFeatureEncoder.encode(row.getArray(index).toDoubleArray())
      case ArrayType(DecimalType(), _) =>
        FloatListFeatureEncoder.encode(
          row.getArray(index).toArray[Decimal](DataTypes.createDecimalType()).map {
            _.toFloat
          })
      case ArrayType(StringType, _) =>
        BytesListFeatureEncoder.encode(row.getArray(index).toArray[UTF8String](StringType).map {
          _.getBytes
        })
      case ArrayType(BinaryType, _) =>
        BytesListFeatureEncoder.encode(row.getArray(index).toArray[Array[Byte]](BinaryType))
      case _ =>
        throw new RuntimeException(
          s"Cannot convert field to unsupported data type ${structField.dataType}")
    }
  }

}
