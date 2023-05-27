package com.calcite.example.example;

import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.sql.*;
import java.util.Properties;

/**
 * 功能：CsvAdapter 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/27 上午9:19
 */
public class CsvAdapterExample {
    public static void main(String[] args) {
        String sql = "select * from test.sales where DEPTNO > 10";
        Connection connection = null;
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            Properties info = new Properties();
            info.setProperty("lex", "JAVA");
            info.setProperty("model", getModelPath("model.json"));
            connection = DriverManager.getConnection("jdbc:calcite:", info);

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                int n = rs.getMetaData().getColumnCount();
                for (int i = 1; i <= n; i++) {
                    String columnName = rs.getMetaData().getColumnLabel(i);
                    Object columnValue = rs.getObject(i);
                    sb.append(i > 1 ? ", " : "").append(columnName).append("=").append(columnValue);
                }
                System.out.println(sb);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 获取模型文件路径
    private static String getModelPath (String model) {
        Source source = Sources.of(CsvAdapterExample.class.getResource("/" + model));
        return source.file().getAbsolutePath();
    }
}
