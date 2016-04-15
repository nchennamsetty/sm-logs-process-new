import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by NChennamsetty on 4/7/2016.
  */

package org.gogoair.info {
object MergeSMParquetFiles {

  def main(args: Array[String]) {
    val inputPath = args(0)
    val outputPath = args(1)
    val numOutPartitions = args(2).toInt
    val fileType = args(3)

    val conf = new SparkConf().setAppName("SM SLA Logs Parquet File Merger")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    fileType match {
      case "csv" => sqlContext.read.parquet (inputPath).coalesce (numOutPartitions).write.format("com.databricks.spark.csv").option("header", "true").save(outputPath)
      case _ => sqlContext.read.parquet (inputPath).coalesce (numOutPartitions).write.parquet (outputPath)
    }

  }
}


}
