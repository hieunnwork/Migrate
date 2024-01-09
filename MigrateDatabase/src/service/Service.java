package service;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Service extends Thread{

    private final String sqlUrl;
    private final String sqlUsername;
    private final String sqlPassword;

    private final String postgresUrl;
    private final String postgresUsername;
    private final String postgresPassword;
    private  final String tableName;
    private  final int numColumns;

    public Service(String sqlUrl, String sqlUsername, String sqlPassword, String postgresUrl, String postgresUsername, String postgresPassword, String tableName, int numColumns) {
        this.sqlUrl = sqlUrl;
        this.sqlUsername = sqlUsername;
        this.sqlPassword = sqlPassword;
        this.postgresUrl = postgresUrl;
        this.postgresUsername = postgresUsername;
        this.postgresPassword = postgresPassword;
        this.tableName = tableName;
        this.numColumns = numColumns;
    }

    @Override
    public void run() {
        SqlServerService sqlServerService= new SqlServerService(sqlUrl,sqlUsername,sqlPassword);
        PostgresService postgresService = new PostgresService(postgresUrl,postgresUsername,postgresPassword);
        int batchSize = 1000;
        int offset = 0;
        List<List<Object>> rows;
        while (true) {
            rows= sqlServerService.SelectRows(tableName,batchSize,offset);
            postgresService.InsertData(tableName,numColumns,rows);
            if (rows.size() % batchSize != 0) {
                break;
            }
            offset += batchSize;
        }
    }

}


