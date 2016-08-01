package com.jufoun.spark.excel

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */
private[excel] class ExcelOptions(@transient private val parameters: Map[String, String]) extends Serializable {

  val charset = parameters.getOrElse("charset", ExcelOptions.DEFAULT_CHARSET)
  val nullValue = parameters.getOrElse("nullValue", ExcelOptions.DEFAULT_NULL_VALUE)
  val codec = parameters.get("codec").orNull
  val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

}

private[excel] object ExcelOptions {
  val DEFAULT_NULL_VALUE = null
  val DEFAULT_CHARSET = "UTF-8"

  def apply(parameters: Map[String, String]): ExcelOptions = new ExcelOptions(parameters)
}