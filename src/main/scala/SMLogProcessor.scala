/**
  * Created by NChennamsetty on 3/30/2016.
  */
import org.apache.spark._
import org.apache.spark.sql._
//import org.apache.spark.sql.hive.HiveContext
import org.gogoair.info.StringUtils._
import com.databricks.spark.csv._
import com.github.nscala_time.time.Imports._



package org.gogoair.info {

  import org.apache.spark.SparkContext

  case class ServiceMetric(tail:String,  acpu_time:String,  latitude:Option[Double], longitude:Option[Double],
                           alt_m:Option[Int], agl_ft:Option[Int], is_spoofing:Boolean, is_in_coverage:Boolean, is_in_regulatory:Boolean,
                           site_id:Option[Int], cell_id:Option[Int], device:String, metric_name:String, metric_value:String, seq_nr:Int,
                           file_name:String, processed_time:String, year:Int, month:Int, day:Int, source:String )

  object SMLogProcessor {

    def main(args: Array[String]) {

      val inputPath = args(0)
      val outputPath = args(1)

      // val outNumPartitions = args(2).toInt
      // val conf = new SparkConf().setAppName("SM SLA Logs Processor").set("spark.kryoserializer.buffer.max", args(3)).set("spark.kryoserializer.buffer", args(4))

      val conf = new SparkConf().setAppName("SM SLA Logs Processor")
      val sc = new SparkContext(conf)

      runSMSLaProcess(sc, inputPath, outputPath)



    }

    def runSMSLaProcess(sc:SparkContext, inputPath:String, outputPath:String): Unit =
    {
      val sqlContext = new SQLContext(sc)



      import sqlContext.implicits._


      val sm_parquet = sqlContext.read.parquet(inputPath)

      // Group by tail and datetime
      val rdd_grps = sm_parquet.rdd.groupBy{
        row =>
          val date_time = row.getString(1)
          val tail = row.getString(2)
          (tail, date_time) }

      // For each group map rows to SM output rows
      val sm_outs = rdd_grps.flatMap {
        grp =>
          val event_fields = scala.collection.mutable.HashMap[String, String]("lat" -> "", "lon" -> "", "alt_m" -> "",
            "agl_ft" -> "", "site_id" ->  "1", "cell_id" -> "1")

          grp._2.foreach {
            r =>
              val event_name = r.getString(3)
              val event_value = r.getString(4)
              event_name match {
                case "Longitude" => event_fields("lon") = event_value
                case "Latitude" => event_fields("lat") = event_value
                case "Altitude(m)" => event_fields("alt_m") = event_value
                case "Altitude(f)" => event_fields("agl_ft") = event_value
                case "AIRCARD_Serving_Sector_ID" =>
                  val sectstr = event_value.replace(" ", "")
                  event_fields("site_id") = (sectstr take(sectstr.length - 2)).toIntOpt.toString
                  event_fields("cell_id") = (sectstr takeRight 2).toIntOpt.toString
                case _ =>

              }


          }

          val fields = List("RTT_Ping", "AIRCARD_ASP_BEST_SINR", "AIRCARD_Tx_power" , "Auth_Pass" , "Total_Pass" , "Active_Open_Pass" )
          val sm_out =  grp._2.filter(row => fields.contains(row.getString(3)) ).map {
            row =>
              //Event Name matches
              row.getString(3) match {
                case "RTT_Ping" =>
                  val pingVals = row.getString(4).replace("ms","").split(" ")
                  pingVals.map {
                    ping => new ServiceMetric(tail = grp._1._1,
                      acpu_time = grp._1._2,
                      latitude = event_fields("lat").toDoubleOpt,
                      longitude = event_fields("lon").toDoubleOpt,
                      alt_m = event_fields("alt_m").toIntOpt,
                      agl_ft = event_fields("agl_ft").toIntOpt,
                      is_in_coverage = true,
                      is_in_regulatory = true,
                      is_spoofing = true,
                      site_id = event_fields("site_id").toIntOpt,
                      cell_id = event_fields("cell_id").toIntOpt,
                      device = "AirCard1",
                      metric_name = "RTT_Ping",
                      metric_value = ping.trim,
                      seq_nr = 1,
                      file_name = row.getString(0),
                      processed_time = DateTime.now.toString,
                      year = row.getInt(7),
                      month=row.getInt(8),
                      day= row.getInt(9),
                      source = row.getString(10)
                    )
                  }.toList

                case "AIRCARD_ASP_BEST_SINR" =>
                  val sinrs = row.getString(4).split(" ")
                  sinrs.map {
                    sinr => new ServiceMetric(tail = grp._1._1,
                      acpu_time = grp._1._2,
                      latitude = event_fields("lat").toDoubleOpt,
                      longitude = event_fields("lon").toDoubleOpt,
                      alt_m = event_fields("alt_m").toIntOpt,
                      agl_ft = event_fields("agl_ft").toIntOpt,
                      is_in_coverage = true,
                      is_in_regulatory = true,
                      is_spoofing = true,
                      site_id = event_fields("site_id").toIntOpt,
                      cell_id = event_fields("cell_id").toIntOpt,
                      device = "AirCard1",
                      metric_name = "AIRCARD_ASP_BEST_SINR",
                      metric_value = sinr.trim,
                      seq_nr = 1,
                      file_name = row.getString(0),
                      processed_time = DateTime.now.toString,
                      year = row.getInt(7),
                      month=row.getInt(8),
                      day= row.getInt(9),
                      source = row.getString(10)
                    )
                  }.toList


                // 1 to 1 metric mappings
                case "AIRCARD_Tx_power" | "Auth_Pass" | "Total_Pass" | "Active_Open_Pass" =>
                  List (new ServiceMetric(tail = grp._1._1,
                    acpu_time = grp._1._2,
                    latitude = event_fields("lat").toDoubleOpt,
                    longitude = event_fields("lon").toDoubleOpt,
                    alt_m = event_fields("alt_m").toIntOpt,
                    agl_ft = event_fields("agl_ft").toIntOpt,
                    is_in_coverage = true,
                    is_in_regulatory = true,
                    is_spoofing = true,
                    site_id = event_fields("site_id").toIntOpt,
                    cell_id = event_fields("cell_id").toIntOpt,
                    device = "",
                    metric_name = row.getString(3),
                    metric_value = row.getString(4),
                    seq_nr = 1,
                    file_name = row.getString(0),
                    processed_time = DateTime.now.toString,
                    year = row.getInt(7),
                    month=row.getInt(8),
                    day= row.getInt(9),
                    source = row.getString(10)
                  )
                  )

              }
          }

          sm_out.toList.flatten

      }


      //import sqlContext.implicits._
      //sm_outs.toDF().coalesce(outNumPartitions).write.parquet(outputPath)


      sm_outs.toDF().write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save(outputPath)
    }

  }

}





