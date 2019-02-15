package org.oneflow.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.oneflow.spark.datasources.SerializableConfiguration

package object functions {

  implicit class RichDataFrameReader(underlay: DataFrameReader) {

    def samplingLimit(n: Int): DataFrameReader = underlay.option("samplingLimit", n)

    def ofrecord(): DataFrameReader = underlay.format("ofrecord")

    def ofrecord(path: String): DataFrame = underlay.ofrecord().load(path)

    def ofrecord(path: String*): DataFrame = underlay.ofrecord().load(path: _*)

    def binary(): DataFrameReader = underlay.format("binary")

    def binary(path: String): DataFrame = underlay.binary().load(path)

    def binary(path: String*): DataFrame = underlay.binary().load(path: _*)

    def chunk(): DataFrameReader = underlay.format("chunk")

    def chunk(path: String): DataFrame = underlay.chunk().load(path)

    def chunk(path: String*): DataFrame = underlay.chunk().load(path: _*)

  }

  implicit class RichDataFrameWriter(underlay: DataFrameWriter[Row]) {

    def ofrecord(): DataFrameWriter[Row] = underlay.format("ofrecord")

    def ofrecord(path: String): Unit = underlay.ofrecord().save(path)

    def chunk(): DataFrameWriter[Row] = underlay.format("chunk")

    def chunk(path: String): Unit = underlay.chunk().save(path)

  }

  implicit class RichDataFrame(underlay: DataFrame) {

    import org.apache.spark.sql.functions._

    def shuffle(): DataFrame = underlay.orderBy(rand())

  }

  implicit class RichSparkContext(underlay: SparkContext) extends Logging {

    def formatFilenameAsOneflowStyle(path: String): Unit = {
      val hadoopPath =  new Path(path)
      val fs = hadoopPath.getFileSystem(underlay.hadoopConfiguration)
      val dir = {
        val files = fs.globStatus(hadoopPath)
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

  implicit class RichBinaryDataset(underlay: Dataset[(String, Array[Byte])]) {
    def saveBinaryToFile(base: String): Unit = {
      val sc = underlay.sparkSession.sparkContext
      val broadcastHadoopConf = sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))

      {
        val basePath = new Path(base)
        val fs = basePath.getFileSystem(broadcastHadoopConf.value.value)
        if (fs.exists(basePath)) {
          require(fs.isDirectory(basePath) && fs.listStatus(basePath).isEmpty)
        } else {
          fs.mkdirs(basePath)
        }
      }
      val broadcastBase = sc.broadcast(base)
      underlay.rdd.foreachPartition {

        _.foreach {
          case (name, data) =>
            val path = Path.mergePaths(new Path(broadcastBase.value), new Path(name))
            val fs = path.getFileSystem(broadcastHadoopConf.value.value)
            val os = fs.create(path, false)
            try {
              os.write(data)
            } finally {
              os.close()
            }
        }
      }
    }
  }

}
