package com.jufoun.spark.excel.util

import java.text.SimpleDateFormat

import com.jufoun.spark.excel.ExcelOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
private[excel] object InferSchema {
  private val logger = LoggerFactory.getLogger(InferSchema.getClass)

  /**
    * 三个阶段,推断出excel记录中集合的类型：
    * 1.推断每一条记录的类型
    * 2.合并类型通过选择最低必需的类型去覆盖相同类型的key
    * 3.任何剩余的空字段替换为字符串，最高类型式
    */
  def apply(tokenRdd:RDD[Array[String]],
            header:Array[String],
            nullValue:String="",
            dateFormatter:SimpleDateFormat=null):StructType={
    val startType:Array[DataType]=Array.fill[DataType](header.length)(NullType)
    val rootType:Array[DataType]=tokenRdd.aggregate(startType)(
      inferRowType(nullValue,dateFormatter),
      mergeRowTypes
    )
    ???
  }

  private def inferRowType(nullValue:String,dateFormatter:SimpleDateFormat)
                          (rowSoFar:Array[DataType],next:Array[String]):Array[DataType]={

  }

  private def mergeRowTypes(
     first:Array[DataType],
     second:Array[DataType]):Array[DataType]={
    ???
  }

}
