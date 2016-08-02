package com.jufoun.spark.excel

import com.jufoun.spark.excel.util.ParserLibs

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */
private[excel] class ExcelOptions(@transient private val parameters: Map[String, String]) extends Serializable {

  val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

  val delimiter=parameters.getOrElse("delimiter",",")
  val quote=parameters.getOrElse("quote","\"")
  val escape=parameters.getOrElse("escape","null")
  val comment=parameters.getOrElse("comment","#")
  val mode=parameters.getOrElse("mode","PERMISSIVE")
  val header=parameters.getOrElse("header","false")
  val parserLib=parameters.getOrElse("parserLib",ParserLibs.DEFAULT)
  val ignoreLeadingWhiteSpace=parameters.getOrElse("ignoreLeadingWhiteSpace","false")
  val ignoreTrailingWhiteSpace=parameters.getOrElse("ignoreTrailingWhiteSpace","false")
  val treatEmptyValuesAsNulls=parameters.getOrElse("treatEmptyValuesAsNulls","false")
  val inferSchema=parameters.getOrElse("inferSchema","false")
  val nullValue = parameters.getOrElse("nullValue", ExcelOptions.DEFAULT_NULL_VALUE)
  val dateFormat = parameters.getOrElse("dateFormat", null)
  val codec = parameters.getOrElse("codec", null)
  val charset = parameters.getOrElse("charset", ExcelOptions.DEFAULT_CHARSET)


}

private[excel] object ExcelOptions {
  val DEFAULT_NULL_VALUE = ""
  val DEFAULT_CHARSET = "UTF-8"
  val DEFAULT_FIELD_DELIMITER=","

  def apply(parameters: Map[String, String]): ExcelOptions = new ExcelOptions(parameters)
}