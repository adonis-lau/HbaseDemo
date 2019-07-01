package bid.adonis.lau.hbase.phoenix.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Phoenix JDBC Connection
 *
 * @Author adonis
 * @Date 2019-07-01 14:18
 * @Description Phoenix JDBC Connection
 */
public class ConnectionManager {

    /**
     * 获取连接
     * @return conn
     * @throws SQLException SQLException
     */
    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
        String url = "jdbc:phoenix:adonis:2181";
//        Class.forName(driver);
        return DriverManager.getConnection(url);
    }
}
