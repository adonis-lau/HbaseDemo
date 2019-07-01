package bid.adonis.lau.hbase.phoenix.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 通过Phoenix使用JDBC操作HBASE
 *
 * @Author adonis
 * @Date 2019-07-01 14:39
 * @Description
 */
@Slf4j
public class PhoenixJdbc {

    public static void createTable(String tableName) {
        log.info("create table {}", tableName);
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.createStatement();
            stmt.executeUpdate("create table  if not exists " + tableName
                    + " (mykey integer not null primary key, mycolumn varchar)");
            conn.commit();


        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }

    }

    public static void addRecord(String tableName, String values) {
        Connection conn = null;
        Statement stmt = null;

        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.createStatement();

            stmt.executeUpdate("upsert into " + tableName + " values (" + values + ")");
            conn.commit();
        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void deleteRecord(String tableName, String whereClause) {
        Connection conn = null;
        Statement stmt = null;

        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.createStatement();

            stmt.executeUpdate("delete from " + tableName + " where "
                    + whereClause);
            conn.commit();
        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void createSequence(String seqName) {
        Connection conn = null;
        Statement stmt = null;

        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.createStatement();

            stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS "
                    + seqName
                    + " START WITH 1000 INCREMENT BY 1 MINVALUE 1000 MAXVALUE 999999999 CYCLE CACHE 30");
            conn.commit();
        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void dropSequence(String seqName) {
        Connection conn = null;
        Statement stmt = null;

        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.createStatement();

            stmt.executeUpdate("DROP SEQUENCE IF EXISTS " + seqName);
            conn.commit();
        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void getAllData(String tableName) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rset = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.prepareStatement("select * from " + tableName);
            rset = stmt.executeQuery();
            while (rset.next()) {
                System.out.print(rset.getInt("mykey"));
                System.out.println(" " + rset.getString("mycolumn"));
            }
        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (rset != null) {
                    rset.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void dropTable(String tableName) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = ConnectionManager.getConnection();
            stmt = conn.createStatement();
            stmt.executeUpdate("drop table  if  exists " + tableName);
            conn.commit();
        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void main(String[] args) {
        createTable("phoenix_test");
        createSequence("PHOENIX_TEST_SEQ_ID");

        // 使用了Sequence
        addRecord("phoenix_test", "NEXT VALUE FOR PHOENIX_TEST_SEQ_ID,'one'");
        addRecord("phoenix_test", "NEXT VALUE FOR PHOENIX_TEST_SEQ_ID,'two'");
        addRecord("phoenix_test", "NEXT VALUE FOR PHOENIX_TEST_SEQ_ID,'three'");

        // deleteRecord("phoenix_test", " mykey = 1 ");
        getAllData("phoenix_test");

        // dropTable("phoenix_test");
//      dropSequence("PHOENIX_TEST_SEQ_ID");

    }
}
