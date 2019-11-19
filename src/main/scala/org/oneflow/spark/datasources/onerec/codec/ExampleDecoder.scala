package org.oneflow.spark.datasources.onerec.codec

import java.nio.ByteBuffer

import onerec._
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
            case xs if xs.nonEmpty => Some(xs.sum)
            case _ => None
          }
          val decoded = tensor.dataType() match {
            case TensorData.Int8List =>
              val list = tensor.data(new Int8List).asInstanceOf[Int8List]
              ArrayData.toArrayData(0.until(list.valuesLength()).map { list.values })
            case TensorData.Int16List =>
              val list = tensor.data(new Int16List).asInstanceOf[Int16List]
              ArrayData.toArrayData(0.until(list.valuesLength()).map { list.values })
            case TensorData.Int32List =>
              val list = tensor.data(new Int32List).asInstanceOf[Int32List]
              ArrayData.toArrayData(0.until(list.valuesLength()).map { list.values })
            case TensorData.Int64List =>
              val list = tensor.data(new Int32List).asInstanceOf[Int64List]
              ArrayData.toArrayData(0.until(list.valuesLength()).map { list.values })
            case TensorData.Float32List =>
              val list = tensor.data(new Float32List).asInstanceOf[Float32List]
              ArrayData.toArrayData(0.until(list.valuesLength()).map { list.values })
            case TensorData.Float64List =>
              val list = tensor.data(new Float64List).asInstanceOf[Float64List]
              ArrayData.toArrayData(0.until(list.valuesLength()).map { list.values })
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
            assert(decoded.array.length == cnt)
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
