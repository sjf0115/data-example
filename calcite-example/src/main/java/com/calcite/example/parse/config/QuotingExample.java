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
 * 日期：2023/5/27 下午11:27
 */
public class QuotingExample {
    public static void main(String[] args) throws SqlParseException {
        // 1. DOUBLE_QUOTE
        // 1.1 使用双引号引用标识符
//        String sql = "SELECT \"id\", \"name\" FROM \"t_user\" WHERE \"id\" > 10";
//        System.out.println("1.1 双引号引用：" + parse(sql, Quoting.DOUBLE_QUOTE));

        // 1.2 使用双引号引用标识符并使用双引号转义
//        sql = "SELECT \"I am a \"\"SQL Boy\"\"\"";
//        System.out.println("1.2 双引号引用并转义：" + parse(sql, Quoting.DOUBLE_QUOTE));

        // 2. DOUBLE_QUOTE_BACKSLASH
        // 使用双引号引用标识符并使用反斜杠转义
//        sql = "SELECT \"my \\\"id\\\"\"";
//        System.out.println("2. 双引号引用并反斜杠转义：" + parse(sql, Quoting.DOUBLE_QUOTE_BACKSLASH));

        // 3. 使用单引号引用标识符 ？
        String sql = "SELECT 'I am a ''SQL Boy'''";
        System.out.println(parse(sql, Quoting.SINGLE_QUOTE));

        // 3. 使用反引号引用标识符
//        sql = "SELECT `id`, `name` FROM `t_user` WHERE `id` > 10";
//        System.out.println(parse(sql, Quoting.BACK_TICK));
    }

    private static String parse(String sql, Quoting quoting) throws SqlParseException {
        // 1. 配置
        SqlParser.Config config = SqlParser.config()
                .withQuoting(quoting)
                .withCaseSensitive(true)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withQuotedCasing(Casing.UNCHANGED)
                .withCharLiteralStyles(ImmutableSet.of(CharLiteralStyle.STANDARD));

        // 2. 解析器
        SqlParser sqlParser = SqlParser.create(sql, config);

        // 3. 解析
        SqlNode sqlNode = sqlParser.parseStmt();
        return sqlNode.toString();
    }
}
