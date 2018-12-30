package org.oneflow.hadoop.ofrecord.io

import java.nio.{ByteBuffer, ByteOrder}

import oneflow.record.OFRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class OFRecordRecordWriter(path: Path, conf: Configuration) extends RecordWriter[Void, OFRecord] {
  private val outputStream =
    path.getFileSystem(conf).create(path, true, OFRecordIOConf.IO_BUFFER_SIZE)
  private val headerBytes = new Array[Byte](8)
  private val headerLengthBuffer =
    ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer()

  override def write(key: Void, value: OFRecord): Unit = {
    val content = value.toByteArray
    headerLengthBuffer.clear()
    headerLengthBuffer.put(content.length)
    outputStream.write(headerBytes)
    outputStream.write(content)
  }

  override def close(context: TaskAttemptContext): Unit = outputStream.close()
}
