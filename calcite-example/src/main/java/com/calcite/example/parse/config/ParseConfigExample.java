package com.calcite.example.parse.config;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * 功能：SQL 解析器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/27 下午10:57
 */
public class ParseConfigExample {
    public static void main(String[] args) throws SqlParseException {
        // 1. 配置
        SqlParser.Config config = SqlParser.config();
        System.out.println(config);

        // 2. 解析器
        String sql = "SELECT id, name FROM t_user WHERE id > 10";
        SqlParser sqlParser = SqlParser.create(sql, config);

        // 3. 解析
        SqlNode sqlNode = sqlParser.parseStmt();
        System.out.println(sqlNode);
    }
}
