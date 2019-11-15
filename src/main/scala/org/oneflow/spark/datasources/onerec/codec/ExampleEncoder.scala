package org.oneflow.spark.datasources.onerec.codec

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import onerec._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{FloatType, StructType, _}

object ExampleEncoder {
  def encode(row: InternalRow, schema: StructType, sortedKeys: Seq[(String, Int)]): Array[Byte] = {
    val builder = new FlatBufferBuilder
    val features = sortedKeys.map {
      case (key, idx) =>
        val field = schema.fields(idx)
        assert(key == field.name)
        val keyOffset = builder.createString(ByteBuffer.wrap(field.name.getBytes("UTF-8")))
        val (tensorDataType, tensorDataOffset) = field.dataType match {
          case ArrayType(FloatType, _) =>
            (
              TensorData.Float32List,
              Float32List.createFloat32List(
                builder,
                Float32List.createValuesVector(builder, row.getArray(idx).toFloatArray())))
          case ArrayType(IntegerType, _) =>
            (
              TensorData.Int32List,
              Int32List.createInt32List(
                builder,
                Int32List.createValuesVector(builder, row.getArray(idx).toIntArray())))
        }
        val shapeOffset = Shape.createShape(builder, Shape.createDimsVector(builder, Array(0)))
        val tensorOffset =
          Tensor.createTensor(builder, shapeOffset, tensorDataType, tensorDataOffset)
        Feature.createFeature(builder, keyOffset, tensorOffset)
    }
    val featuresOffset = Example.createFeaturesVector(builder, features.toArray)
    val exampleOffset = Example.createExample(builder, featuresOffset)
    Example.finishExampleBuffer(builder, exampleOffset)
    builder.sizedByteArray()
  }
}
