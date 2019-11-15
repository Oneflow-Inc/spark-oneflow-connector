package org.oneflow.hadoop.onerec.io

import java.io.EOFException
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import scala.util.{Failure, Success, Try}

class OneRecRecordReader extends RecordReader[Void, Array[Byte]] {

  private var inputStreamOption: Option[FSDataInputStream] = None
  private var progress: Option[Long => Float] = None
  private var current: Option[Array[Byte]] = None

  private val headerBytes = new Array[Byte](16)
  private val headerBuffer =
    ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN)

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
        .open(path, OneRecIOConf.IO_BUFFER_SIZE))
  }

  override def nextKeyValue(): Boolean = {
    current = Try {
      headerBuffer.clear()
      inputStream.readFully(headerBytes)
      (
        headerBuffer.asLongBuffer().get(),
        headerBuffer.asIntBuffer().get(),
        headerBuffer.asIntBuffer().get())
    } match {
      case Success((magic, length, headCrc32)) =>
        val padded_size = if ((length + 4) % 8 == 0) length + 4 else (length + 4) / 8 * 8 + 8
        val pad_size = padded_size - length - 4
        val content = new Array[Byte](length)
        val contentCrc32 = new Array[Byte](4)
        inputStream.readFully(content)
        inputStream.skip(pad_size)
        inputStream.readFully(contentCrc32)
        Some(content)
      case Failure(_: EOFException) =>
        None
      case Failure(e) => throw e
    }
    current.isDefined
  }

  override def getCurrentKey: Void = null

  override def getCurrentValue: Array[Byte] = current.get

  override def getProgress: Float = {
    progress.get(inputStream.getPos)
  }

  override def close(): Unit = {
    inputStreamOption.foreach {
      _.close()
    }
  }

}
