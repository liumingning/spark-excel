package com.jufoun.spark.excel


import com.jufoun.spark.excel.util.{CompressionCodecs, ExcelFile}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Created by HuShiwei on 2016/8/1 0001.
  */
/**
  * 提供纯SQL语句的方式去取Excel数据(提供给用户是JDBC服务)
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider{

  private def checkPath(parameters:Map[String,String]):String={
    parameters.getOrElse("path",sys.error("'path' must be specified for execl data."))
  }

  /**
    * 用给定的参数,将存储在excel中的数据创建一个新的relation
    * 参数必须包含有path
    * @param sqlContext
    * @param parameters
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null)
  }

  /**
    * 用给定的参数和用户提供的schema,将存储在excel中的数据创建一个新的relation
    * 参数必须包含有path
    * @param sqlContext
    * @param parameters
    * @param schema
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path=checkPath(parameters)
    val options=ExcelOptions(parameters)
    val charset=options.charset

    ExcelRelation(
      ()=>ExcelFile.withCharset(sqlContext.sparkContext,path,charset),
      Some(path),
      parameters,
      schema
    )(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }
    if (doSave) {
      val codecClass= CompressionCodecs.getCodecClass(ExcelOptions(parameters).codec)
//将dataframe中的数据存储到excel,待实现
      ???
//      data.saveAsExcelFile(filesystemPath.toString,parameters,codecClass)
    }
    createRelation(sqlContext,parameters,data.schema)
  }
}
