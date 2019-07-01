## 注意事项

#### 打包
如果需要依赖文件,需要在 jdbc/pom.xml 中放出 build 标签的内容  
  
---  
  
#### 控制台打印栈内容:
```
19/07/01 17:15:12 INFO query.ConnectionQueryServicesImpl: HConnection established. Stacktrace for informational purposes: hconnection-0x2a54a73f java.lang.Thread.getStackTrace(Thread.java:1559)
org.apache.phoenix.util.LogUtil.getCallerStackTrace(LogUtil.java:55)
org.apache.phoenix.query.ConnectionQueryServicesImpl.openConnection(ConnectionQueryServicesImpl.java:427)
org.apache.phoenix.query.ConnectionQueryServicesImpl.access$400(ConnectionQueryServicesImpl.java:267)
org.apache.phoenix.query.ConnectionQueryServicesImpl$12.call(ConnectionQueryServicesImpl.java:2515)
org.apache.phoenix.query.ConnectionQueryServicesImpl$12.call(ConnectionQueryServicesImpl.java:2491)
org.apache.phoenix.util.PhoenixContextExecutor.call(PhoenixContextExecutor.java:76)
org.apache.phoenix.query.ConnectionQueryServicesImpl.init(ConnectionQueryServicesImpl.java:2491)
org.apache.phoenix.jdbc.PhoenixDriver.getConnectionQueryServices(PhoenixDriver.java:255)
org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.createConnection(PhoenixEmbeddedDriver.java:150)
org.apache.phoenix.jdbc.PhoenixDriver.connect(PhoenixDriver.java:221)
java.sql.DriverManager.getConnection(DriverManager.java:664)
java.sql.DriverManager.getConnection(DriverManager.java:270)
bid.adonis.lau.hbase.phoenix.jdbc.ConnectionManager.getConnection(ConnectionManager.java:26)
bid.adonis.lau.hbase.phoenix.jdbc.PhoenixJdbc.createTable(PhoenixJdbc.java:26)
bid.adonis.lau.hbase.phoenix.jdbc.PhoenixJdbc.main(PhoenixJdbc.java:213)
```
不是报错, 查看源码:
```
logger.info("HConnection established. Stacktrace for informational purposes: " + connection + " " +  LogUtil.getCallerStackTrace());
```
```
//LogUtil.java
public static String getCallerStackTrace() {
    StackTraceElement[] st = Thread.currentThread().getStackTrace();
    StringBuilder sb = new StringBuilder();
    for (StackTraceElement element : st) {
        sb.append(element.toString());
        sb.append("\n");
    }
    return sb.toString();
}
 ```
此为获取调用栈并记入日志, 不是bug, 是INFO正常日志