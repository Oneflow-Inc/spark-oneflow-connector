package org.oneflow.hadoop.ofrecord.io

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class OFRecordFileOutputFormat extends FileOutputFormat[Void, Array[Byte]] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, Array[Byte]] =
    new OFRecordRecordWriter(getDefaultWorkFile(context, ""), context.getConfiguration)
}
