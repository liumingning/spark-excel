package com.jufoun.spark.excel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */

/**
  * protected[spark] XmlRelation只在spark包下被访问
  * XmlRelation 是case class 意味着使用它的时候可以不需要new ,并且case class 可以带参数，但是object是不可以带参数的
  * () => 这是非严格求值的写法
  */
case class ExcelRelation protected[excel](
    baseRDD:()=>RDD[String],
    location:Option[String],
    parameters:Map[String,String],
    userSchema:StructType=null)(@transient val sqlContext:SQLContext)
  extends BaseRelation
  with InsertableRelation
  with TableScan //把所有的Row对象全部包括到RDD中
  with PrunedScan { // 把所有的Row对象中,消除不需要的列,然后包括到RDD中

  private val logger = LoggerFactory.getLogger(ExcelRelation.getClass)

  private val options = ExcelOptions(parameters)

//  推断RDD的类型类
  override def schema: StructType = {
  Option(userSchema).getOrElse{
    ???
  }
}

  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???

  override def buildScan(): RDD[Row] = ???

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = ???
}
