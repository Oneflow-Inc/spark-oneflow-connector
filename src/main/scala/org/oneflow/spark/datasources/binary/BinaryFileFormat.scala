package org.oneflow.spark.datasources.binary

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.oneflow.spark.datasources.SerializableConfiguration

class BinaryFileFormat extends FileFormat with DataSourceRegister with Logging with Serializable {

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    Some(
      StructType(
        Seq(
          StructField("path", StringType),
          StructField("bytes", BinaryType)
        )))
  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = ???

  override def shortName(): String = "binary"

  override def toString: String = "Binary"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(o: Any): Boolean = o.isInstanceOf[BinaryFileFormat]

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
    file: PartitionedFile => {
      val path = file.filePath
      val stream = FileSystem.get(broadcastedHadoopConf.value.value).open(new Path(path))
      val bytes = try {
        ByteStreams.toByteArray(stream)
      } finally {
        Closeables.close(stream, true)
      }
      Iterator.single().map { _ =>
        InternalRow(UTF8String.fromString(path), bytes)
      }
    }
  }
}
