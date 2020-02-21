package org.oneflow.hadoop.onerec.io

import java.io.EOFException

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.oneflow.onerec.io.OneRecFrameReader

import scala.util.{Failure, Success, Try}

class OneRecRecordReader extends RecordReader[Void, Array[Byte]] {

  private var inputStreamOption: Option[FSDataInputStream] = None
  private var oneRecFrameReaderOption: Option[OneRecFrameReader] = None
  private var progress: Option[Long => Float] = None
  private var current: Option[Array[Byte]] = None

  private def oneRecFrameReader: OneRecFrameReader = oneRecFrameReaderOption.get

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit = split.asInstanceOf[FileSplit]
    val path = fileSplit.getPath
    val epsilon = 1e-6f
    progress = Some({ pos =>
      (pos - fileSplit.getStart).toFloat / (fileSplit.getLength.toFloat + epsilon)
    })

    inputStreamOption = Some(
      path
        .getFileSystem(context.getConfiguration)
        .open(path, OneRecIOConf.IO_BUFFER_SIZE))

    oneRecFrameReaderOption = Some(new OneRecFrameReader(inputStreamOption.get))
  }

  override def nextKeyValue(): Boolean = {
    current = Try{Option(oneRecFrameReader.read())} match {
      case Success(payload) => payload
      case Failure(e) => throw e
    }
    current.isDefined
  }

  override def getCurrentKey: Void = null

  override def getCurrentValue: Array[Byte] = current.get

  override def getProgress: Float = {
    progress.get(inputStreamOption.get.getPos)
  }

  override def close(): Unit = {
    oneRecFrameReaderOption.foreach {
      _.close()
    }
  }

}
