import java.io.{File, FileInputStream, InputStream}
import java.net.URI

import com.jusfoun.excel.ExcelParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by HuShiwei on 2016/7/29 0029.
  */
object test {
  def main(args: Array[String]) {
    val excelParser=new ExcelParser
//    val path= "C:\\jusfoun\\testExcel.xls"
    val filePath: String = "hdfs://192.168.4.202:8020/hsw/testExcel.xls"
    val fs: FileSystem = FileSystem.get(URI.create(filePath), new Configuration)
    var in: InputStream = null
    in = fs.open(new Path(filePath))

//    val file=new File(path)
//    val io=new FileInputStream(file)
    val line=excelParser.parseExcelData(in)
    println(line)
    in.close()
  }

}
