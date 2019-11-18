package org.oneflow.hadoop.onerec.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class OneRecRecordWriter(path: Path, conf: Configuration) extends RecordWriter[Void, Array[Byte]] {
  private val outputStream =
    path.getFileSystem(conf).create(path, false, OneRecIOConf.IO_BUFFER_SIZE)

  private val oneRecFrameWriter = new OneRecFrameCodec().newWriter(outputStream)

  override def write(key: Void, value: Array[Byte]): Unit = {
    oneRecFrameWriter.writeFrame(value)
  }

  override def close(context: TaskAttemptContext): Unit = oneRecFrameWriter.close()
}
