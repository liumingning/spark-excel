package com.jufoun.spark.excel.util

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
    * 拷贝spark的内部api
    * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
    */
  private val numericPrecedence:IndexedSeq[DataType]=
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType,
      DecimalType.Unlimited)

  val findTightestCommonTypeOfTwo:(DataType,DataType)=>Option[DataType]={
    case (t1,t2) if t1==t2 =>Some(t1)

      //数值类型提升到最高的两个
    case (t1,t2) if Seq(t1,t2).forall(numericPrecedence.contains)=>
      val index=numericPrecedence.lastIndexWhere(t=>t==t1||t==t2)
      Some(numericPrecedence(index))

    case _=>None
  }
  /**
    * 三个阶段,推断出excel记录中集合的类型：
    * 1.推断每一条记录的类型
    * 2.合并类型通过选择最低必需的类型去覆盖相同类型的key
    * 3.任何剩余的空字段替换为字符串，最高类型式
    */
/*    def infer(excel:RDD[String],options:ExcelOptions):StructType={
    require(options.samplingRatio>0,s"samplingRatio (${options.samplingRatio}) should be greater than 0")
    val schemaData= if (options.samplingRatio>0.99) {
      excel
    }else{
      excel.sample(withReplacement = false,options.samplingRatio,1)
    }

//    然后在每一行上执行结构推断并且合并
//    schemaData.mapPartitions(iter=>
//    iter.flatMap{excel=>
//
//
//    }
  }*/
}
