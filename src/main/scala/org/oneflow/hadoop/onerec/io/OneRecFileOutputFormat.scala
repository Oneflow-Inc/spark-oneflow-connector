package org.oneflow.hadoop.onerec.io

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class OneRecFileOutputFormat extends FileOutputFormat[Void, Array[Byte]] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, Array[Byte]] =
    new OneRecRecordWriter(getDefaultWorkFile(context, ""), context.getConfiguration)
}
