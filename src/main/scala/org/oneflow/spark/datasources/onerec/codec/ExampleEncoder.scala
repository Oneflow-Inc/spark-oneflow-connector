package org.oneflow.spark.datasources.onerec.codec

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import onerec._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class ExampleEncoder(schema: StructType) {

  private type TensorBuilder = (FlatBufferBuilder, SpecializedGetters) => Int

  private trait TensorDataListBuilder[T] {
    def tensorDataType: Byte
    def buildFn: (FlatBufferBuilder, Array[T]) => Int
  }

  private case class BasicTensorDataListBuilder[T](
      tensorDataType: Byte,
      tableFn: (FlatBufferBuilder, Int) => Int,
      vectorFn: (FlatBufferBuilder, Array[T]) => Int)
      extends TensorDataListBuilder[T] {
    override def buildFn: (FlatBufferBuilder, Array[T]) => Int = { (builder, data) =>
      tableFn(
        builder,
        vectorFn(builder, data)
      )
    }
  }

  private implicit object Int8ListBuilder
      extends BasicTensorDataListBuilder[Byte](
        TensorData.Int8List,
        Int8List.createInt8List,
        Int8List.createValuesVector)

  private implicit object Int16ListBuilder
      extends BasicTensorDataListBuilder[Short](
        TensorData.Int16List,
        Int16List.createInt16List,
        Int16List.createValuesVector)

  private implicit object Int32ListBuilder
      extends BasicTensorDataListBuilder[Int](
        TensorData.Int32List,
        Int32List.createInt32List,
        Int32List.createValuesVector)

  private implicit object Int64ListBuilder
      extends BasicTensorDataListBuilder[Long](
        TensorData.Int64List,
        Int64List.createInt64List,
        Int64List.createValuesVector)

  private implicit object Float32ListBuilder
      extends BasicTensorDataListBuilder[Float](
        TensorData.Float32List,
        Float32List.createFloat32List,
        Float32List.createValuesVector)

  private implicit object Float64ListBuilder
      extends BasicTensorDataListBuilder[Double](
        TensorData.Float64List,
        Float64List.createFloat64List,
        Float64List.createValuesVector)

  private implicit object StringListBuilder extends TensorDataListBuilder[String] {

    override def tensorDataType: Byte = TensorData.StringList

    override def buildFn: (FlatBufferBuilder, Array[String]) => Int = { (builder, data) =>
      StringList.createStringList(builder, StringList.createValuesVector(builder, data.map { s =>
        builder.createString(ByteBuffer.wrap(s.getBytes("UTF-8")))
      }))
    }
  }

  private implicit object BinaryListBuilder extends TensorDataListBuilder[Array[Byte]] {

    override def tensorDataType: Byte = TensorData.BinaryList

    override def buildFn: (FlatBufferBuilder, Array[Array[Byte]]) => Int = { (builder, data) =>
      BinaryList.createBinaryList(builder, BinaryList.createValuesVector(builder, data.map { b =>
        Binary.createBinary(builder, Binary.createBufferVector(builder, b))
      }))
    }
  }

  private class ArrayTensorBuilder[T](arrayGetter: SpecializedGetters => Array[T])(
      implicit listBuilder: TensorDataListBuilder[T])
      extends TensorBuilder {

    def apply(builder: FlatBufferBuilder, row: SpecializedGetters): Int = {
      val data = arrayGetter(row)
      Tensor.createTensor(
        builder,
        Tensor.createShapeVector(builder, Array(data.length)),
        listBuilder.tensorDataType,
        listBuilder.buildFn(builder, data)
      )
    }
  }

  private class NullableTensorBuilder(
      nullable: Boolean,
      isNull: SpecializedGetters => Boolean,
      tensorBuilder: TensorBuilder)
      extends TensorBuilder {
    override def apply(builder: FlatBufferBuilder, row: SpecializedGetters): Int = {
      if (isNull(row)) {
        if (nullable) {
          0
        } else {
          throw new NullPointerException(s"field does not allow null values")
        }
      } else {
        tensorBuilder(builder, row)
      }
    }
  }

  private val sortedTensorBuilder: Array[(String, TensorBuilder)] =
    schema.fields.zipWithIndex.sortBy { _._1.name }.map {
      case (field, ordinal) =>
        val tensorBuilder = field.dataType match {
          case ArrayType(BooleanType, _) =>
            new ArrayTensorBuilder[Byte]((row: SpecializedGetters) => {
              row.getArray(ordinal).toBooleanArray().map { b =>
                if (b) 1.toByte else 0.toByte
              }
            })
          case BooleanType =>
            new ArrayTensorBuilder[Byte]((row: SpecializedGetters) => {
              if (row.getBoolean(ordinal)) {
                Array[Byte](1)
              } else {
                Array[Byte](0)
              }
            })
          case ArrayType(ByteType, _) =>
            new ArrayTensorBuilder[Byte]((row: SpecializedGetters) => {
              row.getArray(ordinal).toByteArray()
            })
          case ByteType =>
            new ArrayTensorBuilder[Byte]((row: SpecializedGetters) => {
              Array(row.getByte(ordinal))
            })
          case ArrayType(ShortType, _) =>
            new ArrayTensorBuilder[Short]((row: SpecializedGetters) => {
              row.getArray(ordinal).toShortArray()
            })
          case ShortType =>
            new ArrayTensorBuilder[Short]((row: SpecializedGetters) => {
              Array(row.getShort(ordinal))
            })
          case ArrayType(IntegerType, _) =>
            new ArrayTensorBuilder[Int]((row: SpecializedGetters) => {
              row.getArray(ordinal).toIntArray()
            })
          case IntegerType =>
            new ArrayTensorBuilder[Int]((row: SpecializedGetters) => {
              Array(row.getInt(ordinal))
            })
          case ArrayType(LongType, _) =>
            new ArrayTensorBuilder[Long]((row: SpecializedGetters) => {
              row.getArray(ordinal).toLongArray()
            })
          case LongType =>
            new ArrayTensorBuilder[Long]((row: SpecializedGetters) => {
              Array(row.getLong(ordinal))
            })
          case ArrayType(FloatType, _) =>
            new ArrayTensorBuilder[Float]((row: SpecializedGetters) => {
              row.getArray(ordinal).toFloatArray()
            })
          case FloatType =>
            new ArrayTensorBuilder[Float]((row: SpecializedGetters) => {
              Array(row.getFloat(ordinal))
            })
          case ArrayType(DoubleType, _) =>
            new ArrayTensorBuilder[Double]((row: SpecializedGetters) => {
              row.getArray(ordinal).toDoubleArray()
            })
          case DoubleType =>
            new ArrayTensorBuilder[Double]((row: SpecializedGetters) => {
              Array(row.getDouble(ordinal))
            })
          case ArrayType(StringType, _) =>
            new ArrayTensorBuilder[String]((row: SpecializedGetters) => {
              row.getArray(ordinal).toArray[UTF8String](StringType).map { _.toString }
            })
          case StringType =>
            new ArrayTensorBuilder[String]((row: SpecializedGetters) => {
              Array(row.getUTF8String(ordinal).toString)
            })
          case ArrayType(BinaryType, _) =>
            new ArrayTensorBuilder[Array[Byte]]((row: SpecializedGetters) => {
              row.getArray(ordinal).toArray[Array[Byte]](BinaryType)
            })
          case BinaryType =>
            new ArrayTensorBuilder[Array[Byte]]((row: SpecializedGetters) => {
              Array(row.getBinary(ordinal))
            })
        }
        (
          field.name,
          new NullableTensorBuilder(
            field.nullable,
            (row: SpecializedGetters) => row.isNullAt(ordinal),
            tensorBuilder))
    }

  def encode(row: InternalRow): Array[Byte] = {
    val builder = new FlatBufferBuilder
    Example.finishExampleBuffer(
      builder,
      Example.createExample(
        builder,
        Example.createFeaturesVector(
          builder,
          sortedTensorBuilder.map {
            case (name, creator) =>
              Feature.createFeature(
                builder,
                builder.createString(ByteBuffer.wrap(name.getBytes("UTF-8"))),
                creator(builder, row))
          }
        )
      )
    )
    builder.sizedByteArray()
  }
}
