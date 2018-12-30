package org.oneflow.hadoop.ofrecord.io

import oneflow.record.OFRecord
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

class OFRecordFileOutputFormat extends FileOutputFormat[Void, OFRecord] {
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Void, OFRecord] =
    new OFRecordRecordWriter(getDefaultWorkFile(context, ""), context.getConfiguration)
}
