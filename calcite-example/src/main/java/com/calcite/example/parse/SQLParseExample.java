package com.calcite.example.parse;

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
        if (fromNode != null) {
            System.out.println("From: " + fromNode);
        }

        // Where
        SqlNode whereNode = sqlSelect.getWhere();
        if (whereNode != null) {
            System.out.println("Where: " + whereNode);
        }

        // Fetch
        SqlNode fetchNode = sqlSelect.getFetch();
        if (fetchNode != null) {
            System.out.println("Fetch: " + fetchNode);
        }

        // Offset
        SqlNode offsetNode = sqlSelect.getOffset();
        if (offsetNode != null) {
            System.out.println("Offset: " + offsetNode);
        }
    }

    private static void parse(String sql) {
        try {
            // 解析配置
            SqlParser.Config config = SqlParser.config()
                    .withLex(Lex.MYSQL);

            // SQL 解析器
            SqlParser sqlParser = SqlParser.create(sql, config);
            // 解析出 SqlNode
            SqlNode sqlNode = sqlParser.parseStmt();
            System.out.println("SQL 类型：" + sqlNode.getKind().lowerName);
            // 查询语句
            if (Objects.equals(sqlNode.getKind(), SqlKind.SELECT)) {
                parseSqlSelect(sqlNode);
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String sql = "select id, name, age FROM student where age < 20";
        parse(sql);
    }
}
