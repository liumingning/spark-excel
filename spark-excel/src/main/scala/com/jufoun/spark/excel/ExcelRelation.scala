package com.jufoun.spark.excel

import java.text.SimpleDateFormat

import com.jufoun.spark.excel.readers.LineExcelReader
import com.jufoun.spark.excel.util.{InferSchema, ParseModes, ParserLibs}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */

/**
  * protected[spark] ExcelRelation只在spark包下被访问
  * ExcelRelation 是case class 意味着使用它的时候可以不需要new ,并且case class 可以带参数，但是object是不可以带参数的
  * () => 这是非严格求值的写法
  */
case class ExcelRelation protected[excel](
   baseRDD: () => RDD[String],//这里很重要.意味着我们首先需要有一个RDD.
   location: Option[String],
   useHeader: Boolean,
   delimiter: Char,
   userSchema: StructType = null,
   inferExcelSchema: Boolean,
   codec: String = null,
   nullValue: String = "",
   dateFormat: String = null)(@transient val sqlContext:SQLContext)
  extends BaseRelation
  with InsertableRelation
  with TableScan //把所有的Row对象全部包括到RDD中
  with PrunedScan { // 把所有的Row对象中,消除不需要的列,然后包括到RDD中

  private val logger = LoggerFactory.getLogger(ExcelRelation.getClass)

//  解析时间类型
  private val dateFormatter= if (dateFormat!=null) {
    new SimpleDateFormat(dateFormat)
  }else{
    null
  }

/*// 判断是否是支持的解析模式
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

//  忽略空白符这个参数不能在用commons这个解析模式中使用
  if ((ignoreLeadingWhiteSpace || ignoreLeadingWhiteSpace) && ParserLibs.isCommonsLib(parserLib)) {
    logger.warn(s"Ignore white space options may not work with Commons parserLib option")
  }

//  判断是否是FAIL_FAST_MODE解析模式,这个快速失败模式是 如果遇到一个畸形的行内容,造成了异常的出现,那么就终止了
  private val failFast = ParseModes.isFailFastMode(parseMode)
  //  判断是否是DROP_MALFORMED_MODE解析模式,这个模式是 当你有的数据比你希望拿到的数据多了或者少了的时候,造成了不匹配这个schema的时候,就删除这些行
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  //  判断是否是PERMISSIVE_MODE解析模式,这个模式是 尽量去解析所有的行.丢失值插入空值和多余的值将被忽略
  private val permissive = ParseModes.isPermissiveMode(parseMode)*/

//  推断RDD的类型类
  override def schema: StructType = inferSchema()

  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???

  override def buildScan(): RDD[Row] = ???

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = ???

  private def inferSchema():StructType= {
    if (userSchema!=null) {
      userSchema
    }else {
      val firstRow =
        new LineExcelReader(
          fieldSep = delimiter
        ).parseLine(firstLine)

      //      是否把第一行当作结构
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
      }
      if (this.inferExcelSchema) {
        val simpleDateFormatter=dateFormatter
        InferSchema(tokenRdd(header),header,nullValue,simpleDateFormatter)

      }
      ???
    }


  }

  /**
    * 从RDD中取出第一行无空的数据
    */
  private lazy val firstLine= {
      baseRDD().filter{line=>
      line.trim.nonEmpty}.first()
  }

//  把与第一行相同的数据全部过滤出去了。剩下的数据集中没有第一行了。
  private def tokenRdd(header:Array[String]):RDD[Array[String]]={

    val filterLine= if (useHeader) firstLine else null

//    如果设置了header,在发送到其他的executors之前确保第一行已经序列化过了
    baseRDD().mapPartitions{iter=>
    val excelIter= if (useHeader) {
      iter.filter(_!=filterLine)
    }else{
      iter
    }
      parseEXCEL(iter)
    }
  }

//  把每一行的String内容,分割后把每个字段存储到Array中了
  private def parseEXCEL(iter:Iterator[String]):Iterator[Array[String]]={
    iter.flatMap {line=>
      val delimiter=ExcelOptions.DEFAULT_FIELD_DELIMITER
      val records=line.split(delimiter).toList

      if (records.isEmpty) {
        logger.warn(s"Ignoring empty line:$line")
        None
      }else{
        Some(records.toArray)
      }

    }
  }

}
