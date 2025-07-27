package org.example;


import org.apache.flink.connector.jdbc.mysql.database.catalog.MySqlCatalog;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.types.DataType;


import java.sql.*;
import java.util.*;

public class MyMySqlCatalog extends MySqlCatalog {


    public MyMySqlCatalog(ClassLoader userClassLoader, String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(userClassLoader, catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    public MyMySqlCatalog(ClassLoader userClassLoader, String catalogName, String defaultDatabase, String baseUrl, Properties connectionProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectionProperties);
    }

    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        // 检查数据库是否存在
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }

        // 检查表是否已存在
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                return;
            }
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(baseUrl + tablePath.getDatabaseName(), this.getUsername(), this.getPassword());

            String createTableSql = generateCreateTableSql(tablePath.getObjectName(), table);

            try (PreparedStatement stmt = conn.prepareStatement(createTableSql)) {
                stmt.execute();
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to create table %s", tablePath.getFullName()), e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private String generateCreateTableSql(String tableName, CatalogBaseTable table) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS `").append(tableName).append("` (");

        // 构建列定义
        Schema schema = table.getUnresolvedSchema();
        List<String> columnDefs = new ArrayList<>();

        for (Schema.UnresolvedColumn column : schema.getColumns()) {
            if (column instanceof Schema.UnresolvedPhysicalColumn) {
                Schema.UnresolvedPhysicalColumn physicalColumn =
                        (Schema.UnresolvedPhysicalColumn) column;
                String columnDef = String.format("`%s` %s",
                        physicalColumn.getName(),
                        convertFlinkTypeToMySql((DataType) physicalColumn.getDataType()));
                columnDefs.add(columnDef);
            }
        }

        sql.append(String.join(", ", columnDefs));
        sql.append(")");

        return sql.toString();
    }

    private String convertFlinkTypeToMySql(DataType dataType) {
        // 简化的类型转换，您可以根据需要扩展
        String typeName = dataType.getLogicalType().getTypeRoot().name();
        switch (typeName) {
            case "INTEGER":
                return "INT";
            case "VARCHAR":
                return "VARCHAR(255)";
            case "BIGINT":
                return "BIGINT";
            case "DOUBLE":
                return "DOUBLE";
            case "BOOLEAN":
                return "BOOLEAN";
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                return "TIMESTAMP";
            default:
                return "TEXT";
        }
    }
}