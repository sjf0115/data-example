package com.calcite.example.parse.config;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * 功能：引用标识符大小写转换
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/27 下午11:43
 */
public class CasingExample {
    public static void main(String[] args) throws SqlParseException {
        // 1. unquotedCasing
        String sql = "SELECT 'id', name FROM t_user WHERE id > 10";
        System.out.println(unquotedCasing(sql, Casing.TO_UPPER));

        // 2. 单引号引用标识符
        sql = "SELECT 'ID', name FROM t_user WHERE id > 10";
        System.out.println(quotedCasing(sql, Casing.TO_LOWER));
    }

    // 标识符不包围
    private static String unquotedCasing(String sql, Casing casing) throws SqlParseException {
        // 1. 配置
        SqlParser.Config config = SqlParser.config()
                .withQuoting(Quoting.BACK_TICK)
                .withUnquotedCasing(casing);

        // 2. 解析器
        SqlParser sqlParser = SqlParser.create(sql, config);

        // 3. 解析
        SqlNode sqlNode = sqlParser.parseStmt();
        return sqlNode.toString();
    }

    private static String quotedCasing(String sql, Casing casing) throws SqlParseException {
        // 1. 配置
        SqlParser.Config config = SqlParser.config()
                .withQuoting(Quoting.BACK_TICK)
                .withQuotedCasing(casing);

        // 2. 解析器
        SqlParser sqlParser = SqlParser.create(sql, config);

        // 3. 解析
        SqlNode sqlNode = sqlParser.parseStmt();
        return sqlNode.toString();
    }
}
