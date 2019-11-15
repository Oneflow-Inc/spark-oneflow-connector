package org.oneflow.hadoop.onerec.io

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class OneRecRecordWriter(path: Path, conf: Configuration) extends RecordWriter[Void, Array[Byte]] {
  private val outputStream =
    path.getFileSystem(conf).create(path, false, OneRecIOConf.IO_BUFFER_SIZE)
  private val headerBytes = new Array[Byte](16)
  private val headerBuffer =
    ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN)

  override def write(key: Void, value: Array[Byte]): Unit = {
    headerBuffer.clear()
    //magic
    headerBuffer.putLong(0)
    headerBuffer.putInt(value.length)
    headerBuffer.putInt(0)
    outputStream.write(headerBytes)
    outputStream.write(value)
    val padded_size = if ((value.length + 4) % 8 == 0) value.length + 4 else (value.length + 4) / 8 * 8 + 8
    val pad_size = padded_size - value.length - 4
    if (pad_size > 0) {
      outputStream.write( new Array[Byte](pad_size))
    }
    outputStream.writeInt(0)
  }

  override def close(context: TaskAttemptContext): Unit = outputStream.close()
}

object OneRecRecordWriterMain extends App {
  private val headerBytes = new Array[Byte](16)
  private val headerBuffer =
    ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN)
  headerBuffer.putLong(1)
  headerBuffer.putLong(2)
  println(headerBuffer.position())


  headerBytes.foreach{println}
}