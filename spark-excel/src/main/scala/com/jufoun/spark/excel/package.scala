package com.jufoun.spark

import org.apache.hadoop.io.compress.CompressionCodec
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
    def ExcelFile(filePath:String):DataFrame={

      //      把xmlFile的输入参数放入Map对象中.parameters里面含有所有需要的参数
      val parameters=Map(
        "path"->filePath
      )
      ???

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
