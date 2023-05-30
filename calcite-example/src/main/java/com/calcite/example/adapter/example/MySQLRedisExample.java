package com.calcite.example.adapter.example;

import com.calcite.example.example.CsvAdapterExample;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.sql.*;
import java.util.Properties;

/**
 * 功能：MySQL 和 Redis 异构数据源混合查询
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/30 上午8:21
 */
public class MySQLRedisExample {
    public static void main(String[] args) {
        String sql = "SELECT a.id, a.name, b.score, a.age, a.email\n" +
                "FROM mysql.tb_user AS a\n" +
                "JOIN redis.user_score AS b\n" +
                "ON a.id = b.id";
        Connection connection = null;
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            Properties info = new Properties();
            info.setProperty("lex", "java");
            info.setProperty("model", getModelPath("hds/mysql_redis_user_model.json"));
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
