package org.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws DatabaseNotExistException, TableAlreadyExistException {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "my_catalog";
        String defaultDatabase = "test";
        String username = "root";
        String password = "root";
        String baseUrl = "jdbc:mysql://localhost:3306";

        MyMySqlCatalog catalog = new MyMySqlCatalog(
                ClassLoader.getSystemClassLoader(),
                name,
                defaultDatabase,
                username,
                password,
                baseUrl

        );
        tableEnv.registerCatalog("my_catalog", catalog);

        // set the JdbcCatalog as the current catalog of the session
        tableEnv.useCatalog("my_catalog");
        List<String> tables = catalog.listTables(defaultDatabase);
        boolean exists = catalog.tableExists(ObjectPath.fromString("test.my_table_03"));
        // 如果表不存在，则创建
        if (!exists) {
            // 定义表的字段和类型
            List<Column> columns = List.of(
                    Column.physical("id", INT().notNull()),
                    Column.physical("name", STRING()));
            Schema.Builder chemaB = Schema.newBuilder();
            chemaB.column("id", INT().notNull());
            chemaB.column("name", STRING());
            chemaB.primaryKey("id");
            Schema chema = chemaB.build();
            CatalogTable catalogTable = CatalogTable.newBuilder()
                    .schema(chema)
                    .build();

            catalog.createTable(ObjectPath.fromString("test.my_table_03"), catalogTable, true);
        }

        tableEnv.executeSql("SELECT * FROM my_table_01").print();
        tableEnv.executeSql("SELECT * FROM my_table_03").print();
        // 执行同步
        tableEnv.executeSql("INSERT INTO my_table_03 SELECT id, name FROM my_table_01");
        System.out.println("Hello World!");
    }
}
