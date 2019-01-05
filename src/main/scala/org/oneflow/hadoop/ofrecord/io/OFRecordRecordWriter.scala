package org.oneflow.hadoop.ofrecord.io

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class OFRecordRecordWriter(path: Path, conf: Configuration) extends RecordWriter[Void, Array[Byte]] {
  private val outputStream =
    path.getFileSystem(conf).create(path, false, OFRecordIOConf.IO_BUFFER_SIZE)
  private val headerBytes = new Array[Byte](8)
  private val headerLengthBuffer =
    ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer()

  override def write(key: Void, value: Array[Byte]): Unit = {
    headerLengthBuffer.clear()
    headerLengthBuffer.put(value.length)
    outputStream.write(headerBytes)
    outputStream.write(value)
  }

  override def close(context: TaskAttemptContext): Unit = outputStream.close()
}
