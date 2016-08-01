package com.jufoun.spark.excel.util

import java.nio.charset.Charset

import com.jufoun.spark.excel.{ExcelInputFormat, ExcelOptions}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */
private[excel] object ExcelFile {
  val DEFAULT_INDENT="  "
  val DEFAULT_ROW_SEPARATOR="\n"
  val DEFAULT_CHARSET="utf-8"

  def withCharset(
    context:SparkContext,
    location:String,
    charset:String=DEFAULT_CHARSET):RDD[String]={
    context.hadoopConfiguration.set(ExcelInputFormat.ENCODING_KEY,charset)
    if (Charset.forName(charset)==Charset.forName(ExcelOptions.DEFAULT_CHARSET)) {
      context.newAPIHadoopFile(location,
        classOf[ExcelInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength))
    } else {
      context.newAPIHadoopFile(location,
        classOf[ExcelInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset))
    }
  }

}
