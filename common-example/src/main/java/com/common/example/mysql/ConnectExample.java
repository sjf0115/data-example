package com.common.example.mysql;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.List;
import java.util.Objects;

/**
 * 功能：连接 MySQL 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/8/7 下午8:52
 */
public class ConnectExample {

    // MySQL Server 8.0 以下版本
//    private static final String DRIVER = "com.mysql.jdbc.Driver";
//    private static final String URL = "jdbc:mysql://localhost:3306/test";

    // MySQL Server 8.0 以上版本
    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    /**
     * 获取 Connection
     * @param user
     * @param password
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static Connection getConn(String user, String password) throws ClassNotFoundException, SQLException {
        // JDBC4 之后不需要再显式通过 Class.forName 注册驱动
        // Class.forName(DRIVER);
        // 获取Connection
        Connection conn = DriverManager.getConnection(URL, user, password);
        return conn;
    }

    /**
     * 执行查询
     * @param conn
     * @param sql
     * @return
     */
    private static List<Student> select(Connection conn, String sql) {
        List<Student> studentList = Lists.newArrayList();
        if (Objects.equals(conn, null) || StringUtils.isBlank(sql)) {
            return studentList;
        }

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while(rs.next()){
                int id  = rs.getInt("id");
                String name = rs.getString("name");
                Student stu = new Student(id, name);
                studentList.add(stu);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (!Objects.equals(stmt, null)) {
                    stmt.close();
                }
                if (!Objects.equals(rs, null)) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return studentList;
    }

    private static class Student {

        public Student(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        private Integer id;
        private String name;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static void main(String[] args) {
        Connection conn = null;
        try {
            // 创建 Conn
            String user = "root";
            String password = "root";
            conn = getConn(user, password);

            // 执行查询
            String sql = "SELECT id, name FROM student";
            List<Student> stuList = select(conn, sql);
            for (Student stu : stuList) {
                System.out.println(stu.getId() + ", " + stu.getName());
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (!Objects.equals(conn, null)) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}

