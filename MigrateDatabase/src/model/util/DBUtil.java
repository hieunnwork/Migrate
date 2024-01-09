package model.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtil {

    public static Connection getConnection(String url, String user, String password) throws SQLException
    {
        return DriverManager.getConnection(url, user,password);
    }
}
