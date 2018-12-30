package org.oneflow.hadoop.ofrecord.io

import java.io.EOFException
import java.nio.{ByteBuffer, ByteOrder}

import oneflow.record.OFRecord
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import scala.util.{Failure, Success, Try}

class OFRecordRecordReader extends RecordReader[Void, OFRecord] {

  private var inputStreamOption: Option[FSDataInputStream] = None
  private var progress: Option[Long => Float] = None
  private var current: Option[OFRecord] = None

  private val headerBytes = new Array[Byte](8)
  private val headerLengthBuffer =
    ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer()

  private def inputStream: FSDataInputStream = inputStreamOption.get

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
        .open(path, OFRecordIOConf.IO_BUFFER_SIZE))
  }

  override def nextKeyValue(): Boolean = {
    current = Try {
      headerLengthBuffer.clear()
      inputStream.readFully(headerBytes)
      headerLengthBuffer.get()
    } match {
      case Success(l) =>
        val content = new Array[Byte](l.toInt)
        inputStream.readFully(content)
        Some(OFRecord.parseFrom(content))
      case Failure(_: EOFException) =>
        None
      case Failure(e) => throw e
    }
    current.isDefined
  }

  override def getCurrentKey: Void = null

  override def getCurrentValue: OFRecord = current.get

  override def getProgress: Float = {
    progress.get(inputStream.getPos)
  }

  override def close(): Unit = {
    inputStreamOption.foreach {
      _.close()
    }
  }

}
