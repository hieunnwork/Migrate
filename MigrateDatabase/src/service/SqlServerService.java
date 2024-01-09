package service;

import model.util.DBUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqlServerService {
    private final String url;
    private final String username;
    private final String password;


    public SqlServerService(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public List<List<Object>> SelectRows (String tableName, int batchSize, int offset) {
        List<List<Object>> rows = new ArrayList<>();
        try {
            Connection connectionSql = DBUtil.getConnection(url, username, password);
            while (true) {
                String query = "SELECT * FROM " + tableName + " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
                PreparedStatement statement = connectionSql.prepareStatement(query);
                statement.setInt(1, offset);
                statement.setInt(2, batchSize);
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    List<Object> rowData = new ArrayList<>();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        Object data = resultSet.getObject(i);
                        rowData.add(data);
                    }
                    rows.add(rowData);
                }

                resultSet.close();
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rows;
    }
}
