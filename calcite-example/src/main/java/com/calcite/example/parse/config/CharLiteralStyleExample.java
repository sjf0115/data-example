package com.calcite.example.parse.config;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/27 下午11:59
 */
public class CharLiteralStyleExample {
    public static void main(String[] args) throws SqlParseException {
        // 1. STANDARD 样式
        String sql = "SELECT 'I''m a student'";
        System.out.println(standardStyle(sql));

        // 2. BQ_DOUBLE 样式
        sql = "SELECT \"I'm a student\"";
        System.out.println(bqDouble(sql));
    }

    // STANDARD 样式
    private static String standardStyle(String sql) throws SqlParseException {
        // 1. 配置
        SqlParser.Config config = SqlParser.config()
                .withCharLiteralStyles(ImmutableSet.of(CharLiteralStyle.STANDARD));

        // 2. 解析器
        SqlParser sqlParser = SqlParser.create(sql, config);

        // 3. 解析
        SqlNode sqlNode = sqlParser.parseStmt();
        return sqlNode.toString();
    }

    // BQ_DOUBLE 样式
    private static String bqDouble(String sql) throws SqlParseException {
        // 1. 配置
        SqlParser.Config config = SqlParser.config()
                .withCharLiteralStyles(ImmutableSet.of(CharLiteralStyle.BQ_DOUBLE));

        // 2. 解析器
        SqlParser sqlParser = SqlParser.create(sql, config);

        // 3. 解析
        SqlNode sqlNode = sqlParser.parseStmt();
        return sqlNode.toString();
    }
}
