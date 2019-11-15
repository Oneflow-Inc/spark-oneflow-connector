package org.oneflow.hadoop.onerec.io

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

class OneRecFileInputFormat extends FileInputFormat[Void, Array[Byte]] {
  override def createRecordReader(
                                   split: InputSplit,
                                   context: TaskAttemptContext): RecordReader[Void, Array[Byte]] =
    new OneRecRecordReader

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}
