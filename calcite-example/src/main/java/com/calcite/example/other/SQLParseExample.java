package com.calcite.example.other;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.Objects;

/**
 * 功能：SQL解析
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/6/18 下午10:35
 */
public class SQLParseExample {

    private static void parseSqlSelect(SqlNode sqlNode) {
        SqlSelect sqlSelect = (SqlSelect) sqlNode;

        // SelectList
        SqlNodeList selectNodeList = sqlSelect.getSelectList();
        for (SqlNode node : selectNodeList.getList()) {
            System.out.println("Select: " + node.toString());
        }

        // OrderList
        SqlNodeList orderList = sqlSelect.getOrderList();
        if (!Objects.equals(orderList, null)) {
            for (SqlNode node : orderList) {
                System.out.println("OrderBy: " + node.toString());
            }
        }

        // GroupList
        SqlNodeList groupList = sqlSelect.getGroup();
        if (!Objects.equals(groupList, null)) {
            for (SqlNode node : groupList) {
                System.out.println("GroupBy: " + node.toString());
            }
        }

        // Hints
        boolean hasHints = sqlSelect.hasHints();
        System.out.println("HasHints: " + hasHints);

        // Window
        SqlNodeList windowList = sqlSelect.getWindowList();
        if (!Objects.equals(windowList, null)) {
            for (SqlNode node : windowList) {
                System.out.println("Window: " + node.toString());
            }
        }

        // From
        SqlNode fromNode = sqlSelect.getFrom();
        System.out.println("From: " + fromNode.toString());

        // Where
        SqlNode whereNode = sqlSelect.getWhere();
        System.out.println("Where: " + whereNode.toString());

        // Fetch
        SqlNode fetchNode = sqlSelect.getFetch();
        System.out.println("Fetch: " + fetchNode.toString());

        // Offset
        SqlNode offsetNode = sqlSelect.getOffset();
        System.out.println("Offset: " + offsetNode.toString());
    }

    private static void parse(String sql) {
        //使用mysql 语法
        SqlParser.Config config = SqlParser.config().withLex(Lex.MYSQL);
        //SqlParser 语法解析器         
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode;
        try {
            sqlNode = sqlParser.parseStmt();
            SqlKind sqlKind = sqlNode.getKind();
            if (sqlKind.equals(SqlKind.SELECT)) {
                parseSqlSelect(sqlNode);
            }
            String name = sqlKind.lowerName;
            System.out.println(name);
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String sql = "select id, name, age FROM student where age < 20";
        sql = "select id as stu_id, name as stu_name from stu where age < 20 and age > 10 order by id limit 10";
        parse(sql);
    }
}
