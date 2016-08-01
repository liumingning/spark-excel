package com.jufoun.spark.excel.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
object readexcel {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("read excel").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val filePath: String = "hdfs://192.168.4.202:8020/hsw/testExcel.xls"

    val recelRDD=ExcelFile.withCharset(sc,filePath)
    recelRDD.foreach(println)
  }

}
