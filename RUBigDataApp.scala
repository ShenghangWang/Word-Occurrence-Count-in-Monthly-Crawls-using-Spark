package org.rubigdata
import org.apache.hadoop.io.NullWritable

import collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLImplicits
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document,Element}
import org.apache.spark.sql.SparkSession

object RUBigDataApp {
  def main(args: Array[String]): Unit = {
    val warcfile = s"hdfs:///opt/hadoop/rubigdata/course_cnbc.warc.gz"
//    val warcfile = s"hdfs:///single-warc-segment"
    val sparkConf = new SparkConf()
      .setAppName("RUBigData WARC4Spark 2021")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[WarcRecord]))
    //                      .set("spark.dynamicAllocation.enabled", "true")
    implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

//    sparkSession.conf.set("spark.dynamicAllocation.enabled", "true")
//    sparkSession.conf.set("spark.executor.cores", 4)
//    sparkSession.conf.set("spark.dynamicAllocation.minExecutors","1")
//    sparkSession.conf.set("spark.dynamicAllocation.maxExecutors","5")

    val warcs = sc.newAPIHadoopFile(
      warcfile,
      classOf[WarcGzInputFormat], // InputFormat
      classOf[NullWritable], // Key
      classOf[WarcWritable] // Value
    )
      //.cache()

    val regex = "^[a-zA-Z0-9]{1,100}$"
    val counter = warcs.map(wr => wr._2.getRecord().getHttpStringBody())
      .map(wb => Jsoup.parse(wb).select("body").text) // <--start of the map phase
      .flatMap(wb => wb.split(" "))
      .filter { wb => wb matches regex } // filter out all the special characters etc
      .filter { wb => wb matches "bear" }
      .map(word => (word, 1)) //<-- end of the map phase
      .reduceByKey { case (x, y) => x + y } // the reduce phase

    counter.take(10).foreach(println)
    sparkSession.stop()
  }
}