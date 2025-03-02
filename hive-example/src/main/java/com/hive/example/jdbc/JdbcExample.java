package com.hive.example.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by wy on 2020/9/13.
 */
public class JdbcExample {

    private static String driverName ="org.apache.hive.jdbc.HiveDriver";
    private static String url="jdbc:hive2://127.0.0.1:10000/default";

    public static void main(String[] args) {
        try {
            Class.forName(driverName);
            Connection conn = DriverManager.getConnection(url,"","");
            String sql = "show tables";
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery();
            int columns= resultSet.getMetaData().getColumnCount();
            int rowIndex = 1;
            while (resultSet.next()) {
                for(int i = 1;i <= columns; i++) {
                    System.out.println("RowIndex: " + rowIndex + ", ColumnIndex: " + i + ", ColumnValue: " + resultSet.getString(i));
                }
            }
        } catch(Exception e)  {
            e.printStackTrace();
        }
    }
}
