package com.common.example.jdbc;

import com.common.example.bean.Student;
import com.google.common.collect.Lists;

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
public class MySQLJDBCExample {

    // MySQL Server 8.0 以下版本
    // private static final String DRIVER = "com.mysql.jdbc.Driver";
    // private static final String URL = "jdbc:mysql://localhost:3306/test";

    // MySQL Server 8.0 以上版本
    private static final String user = "root";
    private static final String password = "root";
    private static final String URL = "jdbc:mysql://localhost:3306/test?useSSL=false&characterEncoding=utf8";

    // 获取数据连接
    private static Connection getConnection() throws SQLException {
        // JDBC4 之后不需要再显式通过 Class.forName 注册驱动
        // Class.forName(DRIVER);
        // 获取Connection
        Connection conn = DriverManager.getConnection(URL, user, password);
        return conn;
    }

    // SELECT：根据姓名查询学生
    public static List<Student> findStudentByName(String stuName) {
        List<Student> students = Lists.newArrayList();
        Connection conn = null;
        try {
            // 获得数据库连接
            conn = getConnection();
            // 查询 SQL
            String sql = "SELECT id, stu_id, stu_name, status FROM tb_student where stu_name = ?";
            // 创建 PreparedStatement
            PreparedStatement statement = conn.prepareStatement(sql);
            // 设置输入参数
            statement.setString(1, stuName);
            // 执行查询
            ResultSet rs = statement.executeQuery();
            // 遍历查询结果
            while(rs.next()){
                int id  = rs.getInt("id");
                int stuId = rs.getInt("stu_id");
                int status  = rs.getInt("status");
                // 从数据库中取出结果并生成 Java 对象
                Student stu = new Student();
                stu.setId(id);
                stu.setStuId(stuId);
                stu.setStuName(stuName);
                stu.setStatus(status);
                students.add(stu);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            // 关闭连接
            try {
                if (!Objects.equals(conn, null)) {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return students;
    }

    // INSERT：创建学生
    public static int createStudent(Student student) {
        Connection conn = null;
        try {
            // 获得数据库连接
            conn = getConnection();
            // 查询 SQL
            String sql = "INSERT INTO tb_student(stu_id, stu_name, status) VALUES(?,?,?)";
            // 创建 PreparedStatement
            PreparedStatement statement = conn.prepareStatement(sql);
            // 设置输入参数
            statement.setInt(1, student.getStuId());
            statement.setString(2, student.getStuName());
            statement.setInt(3, student.getStatus());
            // 执行更新
            int num = statement.executeUpdate();
            return num;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            // 关闭连接
            try {
                if (!Objects.equals(conn, null)) {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        Student stu = new Student();
        stu.setStuId(10005);
        stu.setStuName("阿尔瓦雷斯");
        stu.setStatus(1);
        int num = createStudent(stu);
        if (num == 1) {
            System.out.println("成功创建[阿尔瓦雷斯]学生");
        }

        List<Student> students = findStudentByName("阿尔瓦雷斯");
        for (Student s : students) {
            System.out.println("查询到的用户结果：" + s);
        }
    }
}

