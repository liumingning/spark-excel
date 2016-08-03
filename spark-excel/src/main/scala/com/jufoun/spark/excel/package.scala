package com.jufoun.spark

import com.jufoun.spark.excel.util.ExcelFile
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.Map

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */
package object excel {
  /**
    * Adds a method, `xmlFile`, to [[org.apache.spark.sql.SQLContext]] that allows reading XML data.
    * 运用隐士类的特性，给sqlContext类增加xmlFile方法
    */
  implicit class ExcelContext(sqlContext:SQLContext) extends Serializable{
    def excelFile(
                   filePath:String,
                   useHeader: Boolean = true,//是否把第一行当作结构去解析
                   delimiter: Char = ',',//默认分隔符
                   mode: String = "PERMISSIVE",//解析方式
                   charset: String = ExcelFile.DEFAULT_CHARSET.name(),// 字符编码
                   inferSchema: Boolean = true//是否进行类型推断
                 ):DataFrame={
      val excelRelation=ExcelRelation(
        ()=>ExcelFile.withCharset(sqlContext.sparkContext,filePath,charset),
        location= Some(filePath),
        useHeader=useHeader,
        delimiter=delimiter,
        parseMode= mode,
        treatEmptyValuesAsNulls=true,
        inferExcelSchema = inferSchema
      )(sqlContext)
      sqlContext.baseRelationToDataFrame(excelRelation)

    }


    /**
      * 加一个方法,saveAsExcelFile.一个RDD能够被写到excel数据中
      * 如果compressionCodec不为null，所得到的输出将被压缩。
      * Note that a codec entry in the parameters map will be ignored.
      * @param dataFrame
      */
    implicit class ExcelSchemaRDD(dataFrame: DataFrame){
      def saveAsExcelFile(
        path: String, parameters: Map[String, String] = Map(),
        compressionCodec: Class[_ <: CompressionCodec] = null): Unit ={
//        将dataframe中的数据存储到excel中
        ???
      }
    }
  }

}
