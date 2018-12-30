package org.oneflow.hadoop.ofrecord.io

import oneflow.record.OFRecord
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

class OFRecordFileInputFormat extends FileInputFormat[Void, OFRecord] {
  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[Void, OFRecord] =
    new OFRecordRecordReader

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}
