package com.hive.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by wy on 2020/9/13.
 */
public class JdbcExample {

    private static String driverName ="org.apache.hive.jdbc.HiveDriver";
    private static String Url="jdbc:hive2://127.0.0.1:10000/";

    public static void main(String[] args) {
        try {
            Class.forName(driverName);
            Connection conn = DriverManager.getConnection(Url,"","");
            String sql = "show tables";
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            int columns= resultSet.getMetaData().getColumnCount();
            int rowIndex = 1;
            while (resultSet.next()) {
                for(int i = 1;i <= columns; i++) {
                    System.out.println("RowIndex: " + rowIndex + ", ColumnIndex: " + i + ", ColumnValue: " + resultSet.getString(i));
                }
            }
        }
        catch(Exception e)  {
            e.printStackTrace();
        }
    }
}
