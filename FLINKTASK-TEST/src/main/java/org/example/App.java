package org.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;


/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);


        registerMySqlTable(tableEnv);
        Table table1 = tableEnv.from("my_table_01");
        table1.printSchema();
        tableEnv.executeSql("SELECT * FROM my_table_01").print();


        registerMySqlTable02(tableEnv); // my_table_02
        Table table2 = tableEnv.from("my_table_02");
        table2.printSchema();
        tableEnv.executeSql("SELECT * FROM my_table_02").print();


        // 执行同步
        tableEnv.executeSql("INSERT INTO my_table_02 SELECT id, name FROM my_table_01");
        System.out.println("Hello World!");
    }

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
}
