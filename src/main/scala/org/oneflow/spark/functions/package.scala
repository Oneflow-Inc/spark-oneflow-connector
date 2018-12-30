package org.oneflow.spark

import oneflow.record.OFRecord
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import org.oneflow.hadoop.ofrecord.io.{OFRecordFileInputFormat, OFRecordFileOutputFormat}

package object functions {

  implicit class RichDataFrameReader(underlay: DataFrameReader) {

    def samplingLimit(n: Int): DataFrameReader = underlay.option("samplingLimit", n)

    def ofrecord(): DataFrameReader = underlay.format("ofrecord")

    def ofrecord(path: String): DataFrame = underlay.ofrecord().load(path)

    def ofrecord(path: String*): DataFrame = underlay.ofrecord().load(path: _*)

    def binary(): DataFrameReader = underlay.format("binary")

    def binary(path: String): DataFrame = underlay.binary().load(path)

    def binary(path: String*): DataFrame = underlay.binary().load(path: _*)

  }

  implicit class RichDataFrameWriter(underlay: DataFrameWriter[Row]) {

    def ofrecord(): DataFrameWriter[Row] = underlay.format("ofrecord")

    def ofrecord(path: String): Unit = underlay.ofrecord().save(path)

  }

  implicit class RichDataFrame(underlay: DataFrame) {

    import org.apache.spark.sql.functions._

    def shuffle(): DataFrame = underlay.orderBy(rand())

  }

  implicit class RichSparkContext(underlay: SparkContext) extends Logging {

    def ofRecordFile(path: String): RDD[OFRecord] = {
      underlay
        .newAPIHadoopFile(path, classOf[OFRecordFileInputFormat], classOf[Void], classOf[OFRecord])
        .map(_._2)
    }

    def formatFilenameAsOneflowStyle(path: String): Unit = {
      val fs = FileSystem.get(underlay.hadoopConfiguration)
      val dir = {
        val files = fs.globStatus(new Path(path))
        require(files.length == 1)
        require(files.head.isDirectory)
        files.head
      }
      val from = fs.listStatus(dir.getPath).filter {
        _.getPath.getName.matches("""part\-[0-9]{5}\-.*\.ofrecord""")
      }
      require(from.nonEmpty)
      require(from.forall(_.isFile))
      val ids = from.map {
        _.getPath.getName.substring(5, 10).toInt
      }.distinct
      require(ids.length == from.length)
      require(ids.min == 0)
      require(ids.max == from.length - 1)
      val to = from
        .map {
          _.getPath.getName.substring(0, 10)
        }
        .map { f =>
          new Path(dir.getPath.toString + "/" + f)
        }
      require(!to.exists(fs.exists))

      from.map(_.getPath).zip(to).map {
        case (f, t) =>
          fs.rename(f, t)
      }
    }
  }

  implicit class RichOFRecordRDD(underlay: RDD[OFRecord]) {
    def saveAsOFRecordFile(path: String): Unit = {
      underlay
        .map {
          (null, _)
        }
        .saveAsNewAPIHadoopFile(
          path,
          classOf[Void],
          classOf[OFRecord],
          classOf[OFRecordFileOutputFormat])
    }
  }
}
