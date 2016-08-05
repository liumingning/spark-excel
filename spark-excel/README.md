### 说明
1.spark-excel支持分布式读取Excel2003版和2007版
2.默认取Excel文件的第一行作为结构
3.如果不把第一行作为结构,那么默认取列名为C1,C2,C3.....
4.如果不把第一行作为结构,那么用户可以给StructType
5.支持读取指定sheet页,也可以读取所有sheet页
6.支持指定分隔符
7.支持类型推断
8.如果不做类型推断那么默认就都是String类型
9.支持指定时间类型的格式输出

### 用法

filePath:String,//Excel文件路径
sheetNum:String=ExcelOptions.DEFAULT_SHEET_NUMBER,//解析第几个sheet页
isAllSheet:String=ExcelOptions.DEFAULT_ALL_SHEET,//是否解析所有的sheet页
useHeader: Boolean = ExcelOptions.DEFAULT_USE_HEADER,//是否把第一行当作结构去解析
delimiter: Char = ExcelOptions.DEFAULT_FIELD_DELIMITER.charAt(0),//默认分隔符
mode: String = ExcelOptions.DEFAULT_PARSE_MODE,//解析方式
charset: String = ExcelOptions.DEFAULT_CHARSET,// 字符编码
inferSchema: Boolean = ExcelOptions.DEFAULT_INFERSCHEMA//是否进行类型推断