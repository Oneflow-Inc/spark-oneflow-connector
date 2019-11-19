package org.oneflow.spark.datasources.onerec

import java.net.URI

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
import org.oneflow.hadoop.onerec.io.{OneRecFileInputFormat, OneRecRecordWriter}
import org.oneflow.spark.datasources.SerializableConfiguration
import org.oneflow.spark.datasources.onerec.codec.{ExampleDecoder, ExampleEncoder}

class OneRecFileFormat extends FileFormat with DataSourceRegister with Logging with Serializable {

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val rdd = sparkSession.sparkContext
      .newAPIHadoopFile(
        options("path"),
        classOf[OneRecFileInputFormat],
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
    Some(OneRecInferSchema(sampled))
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = new OutputWriterFactory {

    override def getFileExtension(context: TaskAttemptContext): String = ".onerec"

    override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter =
      new OutputWriter {

        private val encoder = new ExampleEncoder(dataSchema)

        private val writer =
          new OneRecRecordWriter(new Path(new URI(path)), context.getConfiguration)

        override def write(row: InternalRow): Unit = {
          val bytes = encoder.encode(row)
          writer.write(null, bytes)
        }

        override def close(): Unit = writer.close(context)
      }
  }

  override def shortName(): String = "onerec"

  override def toString: String = "OneRec"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(o: Any): Boolean = o.isInstanceOf[OneRecFileFormat]

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def supportDataType(dataType: DataType, isReadPath: Boolean): Boolean = true

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
    file: PartitionedFile =>
      {
        val fileSplit =
          new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext =
          new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)
        val reader =
          new OneRecFileInputFormat().createRecordReader(fileSplit, hadoopAttemptContext)
        reader.initialize(fileSplit, hadoopAttemptContext)
        val decode = new ExampleDecoder(dataSchema)
        new RecordReaderIterator(reader).map { bs =>
          decode.decode(bs)
        }
      }
  }
}
