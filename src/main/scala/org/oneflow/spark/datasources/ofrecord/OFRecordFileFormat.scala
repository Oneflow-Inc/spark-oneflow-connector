package org.oneflow.spark.datasources.ofrecord

import java.net.URI

import oneflow.record.OFRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.oneflow.hadoop.ofrecord.io.{OFRecordFileInputFormat, OFRecordRecordWriter}
import org.oneflow.spark.datasources.SerializableConfiguration
import org.oneflow.spark.datasources.ofrecord.codec.{RowDecoder, RowEncoder}

class OFRecordFileFormat extends FileFormat with DataSourceRegister with Logging with Serializable {

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    val rdd = sparkSession.sparkContext
      .newAPIHadoopFile(
        options("path"),
        classOf[OFRecordFileInputFormat],
        classOf[Void],
        classOf[Array[Byte]])
      .map(_._2)
    val limit = options
      .get("samplingLimit")
      .map {
        _.toInt
      }
      .getOrElse(1024)
    val sampled = sparkSession.sparkContext.makeRDD(rdd.take(limit))
    Some(OFRecordInferSchema(sampled))
  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = new OutputWriterFactory {
    override def getFileExtension(context: TaskAttemptContext): String = ".ofrecord"

    override def newInstance(
                              path: String,
                              dataSchema: StructType,
                              context: TaskAttemptContext): OutputWriter =
      new OutputWriter {
        private val writer =
          new OFRecordRecordWriter(new Path(new URI(path)), context.getConfiguration)

        override def write(row: InternalRow): Unit = {
          val record = RowEncoder.encode(row, dataSchema)
          writer.write(null, record.toByteArray)
        }

        override def close(): Unit = writer.close(context)
      }
  }

  override def shortName(): String = "ofrecord"

  override def toString: String = "OFRecord"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(o: Any): Boolean = o.isInstanceOf[OFRecordFileFormat]

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = false

  override protected def buildReader(
                                      sparkSession: SparkSession,
                                      dataSchema: StructType,
                                      partitionSchema: StructType,
                                      requiredSchema: StructType,
                                      filters: Seq[Filter],
                                      options: Map[String, String],
                                      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    file: PartitionedFile => {
      val fileSplit =
        new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      val reader =
        new OFRecordFileInputFormat().createRecordReader(fileSplit, hadoopAttemptContext)
      reader.initialize(fileSplit, hadoopAttemptContext)
      new RecordReaderIterator(reader).map { bs =>
        RowDecoder.decode(OFRecord.parseFrom(bs), requiredSchema)
      }
    }
  }
}
