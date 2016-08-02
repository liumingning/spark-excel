/*
package com.jufoun.spark.excel


import com.jufoun.spark.excel.util.{CompressionCodecs, ExcelFile, ParserLibs, TypeCast}
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
class DefaultSourceBak extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider{

  private def checkPath(parameters:Map[String,String]):String={
    parameters.getOrElse("path",sys.error("'path' must be specified for EXCEL data."))
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
    val options=ExcelOptions(parameters)
    val path=checkPath(parameters)
//    获取分隔符
    val delimiter=TypeCast.toChar(options.delimiter)
//    获取引用符号
    val quote=options.quote
    val quoteCharacter= if (quote==null) {
      null
    }else if (quote.length==1) {
      quote.charAt(0)
    }else{
      throw new Exception("Quotation cannot be more than one character.")
    }

//    获取escape
    val escape= options.escape
    val escapeChar:Character= if (escape==null) {
      null
    }else if (escape.length==1) {
      escape.charAt(0)
    }else{
      throw new Exception("Escape character cannot be more than one character.")
    }

//    skip lines beginning with this character. Default is "#". Disable comments by setting this to null.
    val comment=options.comment
    val commentChar: Character = if (comment == null) {
      null
    } else if (comment.length == 1) {
      comment.charAt(0)
    } else {
      throw new Exception("Comment marker cannot be more than one character.")
    }

//    获取解析的模式
    val parseMode = options.mode

//    是否把第一行当做结构
    val useHeader = options.header
    val headerFlag= if (useHeader=="true") {
      true
    }else if (useHeader=="false") {
      false
    }else{
      throw new Exception("Header flag can be true or false")
    }

    val parserLib = options.parserLib

    val ignoreLeadingWhiteSpace = parameters.getOrElse("ignoreLeadingWhiteSpace", "false")
    val ignoreLeadingWhiteSpaceFlag = if (ignoreLeadingWhiteSpace == "false") {
      false
    } else if (ignoreLeadingWhiteSpace == "true") {
      if (!ParserLibs.isUnivocityLib(parserLib)) {
        throw new Exception("Ignore whitesspace supported for Univocity parser only")
      }
      true
    } else {
      throw new Exception("Ignore white space flag can be true or false")
    }

    val ignoreTrailingWhiteSpace = options.ignoreTrailingWhiteSpace
    val ignoreTrailingWhiteSpaceFlag = if (ignoreTrailingWhiteSpace == "false") {
      false
    } else if (ignoreTrailingWhiteSpace == "true") {
      if (!ParserLibs.isUnivocityLib(parserLib)) {
        throw new Exception("Ignore whitespace supported for the Univocity parser only")
      }
      true
    } else {
      throw new Exception("Ignore white space flag can be true or false")
    }

    val treatEmptyValuesAsNulls = options.treatEmptyValuesAsNulls
    val treatEmptyValuesAsNullsFlag = if (treatEmptyValuesAsNulls == "false") {
      false
    } else if (treatEmptyValuesAsNulls == "true") {
      true
    } else {
      throw new Exception("Treat empty values as null flag can be true or false")
    }

    val inferSchema = options.inferSchema
    val inferSchemaFlag = if (inferSchema == "false") {
      false
    } else if (inferSchema == "true") {
      true
    } else {
      throw new Exception("Infer schema flag can be true or false")
    }
    val nullValue = parameters.getOrElse("nullValue", "")

    val dateFormat = parameters.getOrElse("dateFormat", null)

    val codec = parameters.getOrElse("codec", null)


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
*/
