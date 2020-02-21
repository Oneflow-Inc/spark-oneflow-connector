package org.oneflow.spark.datasources.onerec.codec

import java.nio.{ByteBuffer, ByteOrder}

import onerec.example._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

class ExampleDecoder(schema: StructType) {

  private type FieldDecoder = Example => Any

  private val fieldDecodes: Seq[FieldDecoder] = schema.fields.map { field => (example: Example) =>
    {
      Option(example.featuresByKey(field.name)).flatMap { f =>
        Option(f.tensor())
      } match {
        case None =>
          if (field.nullable) {
            null
          } else {
            throw new NullPointerException(s"Field ${field.name} does not allow null values")
          }
        case Some(tensor) =>
          val elementCount = 0.until(tensor.shapeLength()).map {
            tensor.shape
          } match {
            case xs if xs.nonEmpty => Some(xs.product)
            case _ => None
          }
          val decoded = tensor.dataType() match {
            case TensorData.Int8List =>
              val list = tensor.data(new Int8List).asInstanceOf[Int8List]
              val buf = new Array[Byte](list.valuesLength())
              list.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).get(buf)
              ArrayData.toArrayData(buf)
            case TensorData.Int16List =>
              val list = tensor.data(new Int16List).asInstanceOf[Int16List]
              val buf = new Array[Short](list.valuesLength())
              list.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(buf)
              ArrayData.toArrayData(buf)
            case TensorData.Int32List =>
              val list = tensor.data(new Int32List).asInstanceOf[Int32List]
              val buf = new Array[Int](list.valuesLength())
              list.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(buf)
              ArrayData.toArrayData(buf)
            case TensorData.Int64List =>
              val list = tensor.data(new Int64List).asInstanceOf[Int64List]
              val buf = new Array[Long](list.valuesLength())
              list.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().get(buf)
              ArrayData.toArrayData(buf)
            case TensorData.Float32List =>
              val list = tensor.data(new Float32List).asInstanceOf[Float32List]
              val buf = new Array[Float](list.valuesLength())
              list.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(buf)
              ArrayData.toArrayData(buf)
            case TensorData.Float64List =>
              val list = tensor.data(new Float64List).asInstanceOf[Float64List]
              val buf = new Array[Double](list.valuesLength())
              list.valuesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().get(buf)
              ArrayData.toArrayData(buf)
            case TensorData.StringList =>
              val list = tensor.data(new StringList).asInstanceOf[StringList]
              ArrayData.toArrayData(0.until(list.valuesLength()).map { list.values })
            case TensorData.BinaryList =>
              val list = tensor.data(new BinaryList).asInstanceOf[BinaryList]
              ArrayData.toArrayData(0.until(list.valuesLength()).map { list.values }.map { b =>
                0.until(b.bufferLength()).map { b.buffer }
              })
          }
          elementCount.foreach { cnt =>
            assert(decoded.numElements() == cnt)
          }
          decoded
      }
    }
  }

  def decode(data: Array[Byte]): InternalRow = {
    val example = Example.getRootAsExample(ByteBuffer.wrap(data))
    InternalRow.fromSeq(
      fieldDecodes.map { _(example) }
    )
  }
}
