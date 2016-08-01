package com.jufoun.spark.excel.util

import java.io.InputStream

import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.Cell
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer, StringBuilder}


/**
  * Created by HuShiwei on 2016/7/29 0029.
  */

/**
  * 解析xls文件，利用poi对文件进行解析
  * Excel解析类，读取每一行并转成字符串输出以换行符分割，每一行的字段以\t分割，最终是一串字符串
  */
object ExcelParser {
  private val logger = LoggerFactory.getLogger(ExcelParser.getClass)
  private var currntString: StringBuilder = _
  private var bytesRead = 0

  def parseExcelData(is: InputStream): Array[String] = {
    val resultList=new ListBuffer[String]()

    val workbook = new HSSFWorkbook(is)
    val sheet = workbook.getSheetAt(0)
    val rowIterator = sheet.iterator()
    while (rowIterator.hasNext) {
    currntString = new StringBuilder()
      val row = rowIterator.next()
      val cellIterator = row.cellIterator()
      while (cellIterator.hasNext) {

        val cell = cellIterator.next()
        if (null != cell) {
          cell.getCellType match {
            case Cell.CELL_TYPE_BOOLEAN => {
              bytesRead += 1
              currntString.append(cell.getBooleanCellValue + "\t")
            }
            case Cell.CELL_TYPE_NUMERIC => {
              bytesRead += 1
              currntString.append(cell.getNumericCellValue + "\t")
            }
            case Cell.CELL_TYPE_STRING => {
              bytesRead += 1
              currntString.append(cell.getStringCellValue + "\t")
            }
            case Cell.CELL_TYPE_FORMULA => {
              bytesRead += 1
              currntString.append(cell.getCellFormula + "\t")
            }
            case Cell.CELL_TYPE_BLANK => {
              bytesRead += 1
              currntString.append("" + "\t")
            }
            case Cell.CELL_TYPE_ERROR => {
              bytesRead += 1
              currntString.append("非法字符" + "\t")
            }
            case _ => {
              bytesRead += 1
              currntString.append("未知类型" + "\t")
            }
          }
        }
      }
      resultList.append(currntString.toString())
    }
    is.close()
    return resultList.toArray
  }
  def getBytesRead={
    bytesRead
  }

}
