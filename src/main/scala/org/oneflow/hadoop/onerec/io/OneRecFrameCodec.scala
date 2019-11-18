package org.oneflow.hadoop.onerec.io

import java.io.{EOFException, IOException, InputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder}

import net.jpountz.xxhash.XXHashFactory

import scala.util.{Failure, Success, Try}

object OneRecFrameParseException extends IOException

private object OneRecFrameHeader {
  val MAGIC_LENGTH: Int = 8
  val MAGIC: Array[Byte] = "^ONEREC$".getBytes
  assert(MAGIC_LENGTH == MAGIC.length)
  val MAGIC_NUMBER: Long = ByteBuffer.wrap(MAGIC).order(ByteOrder.LITTLE_ENDIAN).getLong
  val RESERVED_LENGTH: Int = 4
  val RESERVED: Array[Byte] = Array.fill[Byte](RESERVED_LENGTH) { 0 }
  val CONTENT_SIZE_LENGTH: Int = 4
  val CHECKSUM_LENGTH: Int = 8
  val HEADER_LENGTH_WITHOUT_CHECKSUM: Int = MAGIC_LENGTH + RESERVED_LENGTH + CONTENT_SIZE_LENGTH
  val HEADER_LENGTH: Int = HEADER_LENGTH_WITHOUT_CHECKSUM + CHECKSUM_LENGTH

  def fromBytes(buf: Array[Byte], off: Int, len: Int): Try[OneRecFrameHeader] = {
    (if (len == HEADER_LENGTH) Success() else Failure(OneRecFrameParseException)).map { _ =>
      val buffer = ByteBuffer.wrap(buf, off, len).order(ByteOrder.LITTLE_ENDIAN)
      (buffer.getLong, buffer.getInt, buffer.getInt, buffer.getLong)
    } flatMap {
      case (MAGIC_NUMBER, 0, size, checksum) if size > 0 =>
        if (OneRecFrameCodec.hash64.hash(buf, 0, HEADER_LENGTH_WITHOUT_CHECKSUM, 0) == checksum)
          Success(new OneRecFrameHeader(size))
        else Failure(OneRecFrameParseException)
      case _ => Failure(OneRecFrameParseException)
    }
  }

  def fromBytes(buf: Array[Byte]): Try[OneRecFrameHeader] = {
    fromBytes(buf, 0, buf.length)
  }
}

private class OneRecFrameHeader(val contentSize: Int) {
  def toBytes(buf: Array[Byte], off: Int, len: Int): Unit = {
    if (len != OneRecFrameHeader.HEADER_LENGTH) {
      throw new IllegalArgumentException
    }
    val buffer = ByteBuffer.wrap(buf, off, len).order(ByteOrder.LITTLE_ENDIAN)
    buffer.put(OneRecFrameHeader.MAGIC)
    buffer.put(OneRecFrameHeader.RESERVED)
    buffer.putInt(contentSize)
    buffer.putLong(
      OneRecFrameCodec.hash64.hash(buf, 0, OneRecFrameHeader.HEADER_LENGTH_WITHOUT_CHECKSUM, 0))
  }
  def toBytes(buf: Array[Byte]): Unit = {
    toBytes(buf, 0, buf.length)
  }
}

trait OneRecFrameWriter extends AutoCloseable {
  def writeFrame(content: Array[Byte], off: Int, len: Int): Unit
  def writeFrame(content: Array[Byte]): Unit = {
    writeFrame(content, 0, content.length)
  }
}

trait OneRecFrameReader extends AutoCloseable {
  def readFrame(): Try[Array[Byte]]
}

object OneRecFrameCodec {
  val CONTENT_ALIGN_SIZE: Int = 8

  def getPadSize(len: Int): Int = {
    len % CONTENT_ALIGN_SIZE
  }

  private[io] val hash64 = XXHashFactory.fastestInstance().hash64()
}

class OneRecFrameCodec {

  def newWriter(os: OutputStream): OneRecFrameWriter = new OneRecFrameWriter {
    private val headerBytes = new Array[Byte](OneRecFrameHeader.HEADER_LENGTH)
    private val checksumBytes = new Array[Byte](OneRecFrameHeader.CHECKSUM_LENGTH)
    private val checksumBuffer =
      ByteBuffer.wrap(checksumBytes).order(ByteOrder.LITTLE_ENDIAN)
    private val padBytes = Array.fill[Byte](OneRecFrameCodec.CONTENT_ALIGN_SIZE) { 0 }
    override def writeFrame(content: Array[Byte], off: Int, len: Int): Unit = {
      new OneRecFrameHeader(content.length).toBytes(headerBytes)
      os.write(headerBytes)
      os.write(content, off, len)
      os.write(padBytes, 0, OneRecFrameCodec.getPadSize(len))
      checksumBuffer.reset()
      checksumBuffer.putLong(OneRecFrameCodec.hash64.hash(ByteBuffer.wrap(content), 0))
      os.write(checksumBytes)
    }

    override def close(): Unit = {
      os.close()
    }
  }

  def newReader(is: InputStream): OneRecFrameReader = new OneRecFrameReader {
    private val headerBytes = new Array[Byte](OneRecFrameHeader.HEADER_LENGTH)
    private val checksumBytes = new Array[Byte](OneRecFrameHeader.CHECKSUM_LENGTH)
    private val checksumBuffer =
      ByteBuffer.wrap(checksumBytes).order(ByteOrder.LITTLE_ENDIAN)

    private def readFullyOrEOF(buf: Array[Byte], off: Int, len: Int): Try[Unit] = {
      Try {
        is.read(buf, off, len)
      }.flatMap {
        case `len` => Success()
        case -1 => Failure(new EOFException())
        case _ => Failure(OneRecFrameParseException)
      }
    }

    private def readFullyOrEOF(buf: Array[Byte]): Try[Unit] = {
      readFullyOrEOF(buf, 0, buf.length)
    }

    private def readFully(buf: Array[Byte], off: Int, len: Int): Try[Unit] = {
      readFullyOrEOF(buf, off, len).recoverWith {
        case _: EOFException => Failure(OneRecFrameParseException)
      }
    }

    private def readFully(buf: Array[Byte]): Try[Unit] = {
      readFully(buf, 0, buf.length)
    }

    private def skipFully(n: Long): Try[Unit] = {
      Try { is.skip(n) }
        .flatMap {
          case `n` => Success()
          case _ => Failure(OneRecFrameParseException)
        }
        .recoverWith {
          case _: EOFException => Failure(OneRecFrameParseException)
        }
    }

    override def readFrame(): Try[Array[Byte]] = {
      readFullyOrEOF(headerBytes)
        .flatMap { _ =>
          OneRecFrameHeader.fromBytes(headerBytes)
        }
        .flatMap { header =>
          val content = new Array[Byte](header.contentSize)
          readFully(content)
            .flatMap { _ =>
              skipFully(OneRecFrameCodec.getPadSize(header.contentSize))
            }
            .flatMap { _ =>
              readFully(checksumBytes)
            }
            .flatMap { _ =>
              checksumBuffer.reset()
              if (checksumBuffer.getLong == OneRecFrameCodec.hash64
                  .hash(ByteBuffer.wrap(content), 0)) Success()
              else Failure(OneRecFrameParseException)
            }
            .map { _ =>
              content
            }
        }
    }

    override def close(): Unit = {
      is.close()
    }
  }
}
