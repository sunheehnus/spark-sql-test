package com.redislabs.test

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

case class SCAN(parameters: Map[String, String])
               (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with colContents {

  val schema: StructType = StructType(StructField("instant", StringType, nullable = true) +: (0 to colnum).map(_.toString).map(StructField(_, IntegerType, nullable = true)))

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val rdd = sqlContext.sparkContext.parallelize(1 to 2048, 3)
    val mappedrdd = rdd.map(x =>(x.toString, (x to x + colnum).toSeq.toArray))
    mappedrdd.map(x => Row.fromSeq(x._1 +: x._2))
  }
}


class dataframeRP extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    SCAN(parameters)(sqlContext)
  }
}

object run extends App with colContents {
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val rdd = sqlContext.sparkContext.parallelize(1 to 2048, 3)
  val mappedrdd = rdd.map(x =>(x.toString, (x to x + colnum).toSeq.toArray))
  val df = mappedrdd.toDF()

  val dataColExpr = (0 to colnum).map(_.toString).zipWithIndex.map { case (key, i) => s"_2[$i] AS `$key`" }
  val allColsExpr = "_1 AS instant" +: dataColExpr

  val df1 = df.selectExpr(allColsExpr: _*)

  val cmpdf = sqlContext.load("com.redislabs.test.dataframeRP", Map[String, String]())

  val t1 = System.currentTimeMillis()
  df1.collect
  val t2 = System.currentTimeMillis()
  val t3 = System.currentTimeMillis()
  cmpdf.collect
  val t4 = System.currentTimeMillis()

  println(t2 - t1)
  println(t4 - t3)
}

trait colContents {
  val colnum = 2048
}
