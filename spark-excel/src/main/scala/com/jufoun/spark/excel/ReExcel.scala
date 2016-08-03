package com.jufoun.spark.excel

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/8/3 0003.
  */
object ReExcel {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("read excel").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val filePath: String = "hdfs://192.168.4.202:8020/hsw/testExcel.xls"

    val excelDF=sqlContext.excelFile(filePath)
    excelDF.printSchema()
    excelDF.show()
    sc.stop()

  }

}
