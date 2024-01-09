package service;

import model.util.DBUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PostgresService {

    private final String url;
    private final String username;
    private final String password;


    public PostgresService(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public static String generateInsertStatement(String tableName, int numColumns) {
        String columns = IntStream.range(1, numColumns + 1)
                .mapToObj(i -> "column" + i)
                .collect(Collectors.joining(", "));

        String values = IntStream.range(1, numColumns + 1)
                .mapToObj(i -> "?")
                .collect(Collectors.joining(", "));

        String insertStatement = String.format("INSERT INTO %s (%s) VALUES (%s);", tableName, columns, values);

        return insertStatement;
    }
    public void InsertData(String tableName, int numColumns, List<List<Object>> rows ){
        try (Connection connection = DBUtil.getConnection(url, username, password)) {
            String sql= generateInsertStatement(tableName,numColumns);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (List<Object> rowData : rows) {
                for (int i = 0; i < rowData.size(); i++) {
                    Object data = rowData.get(i);
                    if (data == null) {
                        preparedStatement.setObject(i + 1, null);
                    } else if (data instanceof String) {
                        preparedStatement.setString(i + 1, (String) data);
                    } else if (data instanceof Integer) {
                        preparedStatement.setInt(i + 1, (Integer) data);
                    } else if (data instanceof Double) {
                        preparedStatement.setDouble(i + 1, (Double) data);
                    } else if (data instanceof Boolean) {
                        preparedStatement.setBoolean(i + 1, (Boolean) data);
                    } else if (data instanceof java.sql.Date) {
                        preparedStatement.setDate(i + 1, (java.sql.Date) data);
                    } else if (data instanceof java.sql.Timestamp) {
                        preparedStatement.setTimestamp(i + 1, (java.sql.Timestamp) data);
                    }
                }
                preparedStatement.executeUpdate();
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
