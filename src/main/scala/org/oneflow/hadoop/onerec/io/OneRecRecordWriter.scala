package org.oneflow.hadoop.onerec.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.oneflow.onerec.io.OneRecFrameWriter

class OneRecRecordWriter(path: Path, conf: Configuration) extends RecordWriter[Void, Array[Byte]] {
  private val outputStream =
    path.getFileSystem(conf).create(path, false, OneRecIOConf.IO_BUFFER_SIZE)

  private val oneRecFrameWriter = new OneRecFrameWriter(outputStream)

  override def write(key: Void, value: Array[Byte]): Unit = {
    oneRecFrameWriter.write(value)
  }

  override def close(context: TaskAttemptContext): Unit = oneRecFrameWriter.close()
}
