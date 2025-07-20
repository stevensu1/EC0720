package org.example;

import org.apache.flink.table.api.TableEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TaskDay01 {

    /**
     * 注册 MySQL 表 my_table_02 到 Flink 表环境中
     */
    public static void registerMySqlTable02(TableEnvironment tableEnv) {
        tableEnv.executeSql(
                "CREATE TABLE my_table_02 (" +
                        "id INT PRIMARY KEY NOT ENFORCED, " +
                        "name STRING" +
                        ") WITH (" +
                        "'connector' = 'jdbc', " +
                        "'url' = 'jdbc:mysql://localhost:3306/test', " +
                        "'table-name' = 'my_table_02', " +
                        "'username' = 'root', " +
                        "'password' = 'root'" +
                        ")"
        );
    }

    /**
     * 注册 MySQL 表到 Flink 表环境中
     */
    public static void registerMySqlTable(TableEnvironment tableEnv) {
        tableEnv.executeSql(
                "CREATE TABLE my_table_01 (" +
                        "id INT PRIMARY KEY NOT ENFORCED," +
                        "name STRING" +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = 'jdbc:mysql://localhost:3306/test'," +
                        "'table-name' = 'my_table_01'," +
                        "'username' = 'root'," +
                        "'password' = 'root'" +
                        ")"
        );
    }


    /**
     * 创建表，如果表不存在则创建
     */
    public static void createTableIfNotExists(String tableName) {
        String url = "jdbc:mysql://localhost:3306/test";
        String user = "root";
        String password = "root";

        try (
                Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement()) {
            String sql = String.format("""
                                 CREATE TABLE IF NOT EXISTS %s (
                                 id INT PRIMARY KEY, 
                                 name VARCHAR(255))
                            """,
                    tableName
            );
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 向 JDBC 表my_table_01 中写入10条数据
    public static void insertDataIntoTable() {
        String url = "jdbc:mysql://localhost:3306/test";
        String user = "root";
        String password = "root";

        try (
                Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement()) {
            for (int i = 1; i <= 10; i++) {
                String sql = "INSERT INTO my_table_01 VALUES (" + i + ", 'name_" + i + "')";
                stmt.executeUpdate(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
