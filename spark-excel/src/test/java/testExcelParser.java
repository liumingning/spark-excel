import com.jusfoun.excel.ExcelParser;

import java.io.*;

/**
 * Created by HuShiwei on 2016/7/29 0029.
 */
public class testExcelParser {
    public static void main(String[] args) throws IOException {
        ExcelParser excelParser = new ExcelParser();
        /** 读取本地文件用这个 */
        String filePath = "C:\\jusfoun\\userinfo.xls";
        File file = new File(filePath);
//
        InputStream fileInputStream = new FileInputStream(file);
        String line = excelParser.parseExcelData(fileInputStream);
        System.out.println(line);
        fileInputStream.close();
    }
}
