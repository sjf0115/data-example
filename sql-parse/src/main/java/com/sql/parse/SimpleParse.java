package com.sql.parse;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

/**
 * 功能：Flink SQL 解析入门
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/6/12 23:37
 */
public class SimpleParse {
    public static void main(String[] args) {
        String sql = "SELECT\n" +
                "  a.id, b.name, a.score\n" +
                "FROM (\n" +
                "  SELECT id, score \n" +
                "  FROM senior_stu\n" +
                "  WHERE score > 90\n" +
                "  UNION ALL \n" +
                "  SELECT id, score \n" +
                "  FROM middle_sut\n" +
                "  WHERE score > 90\n" +
                ") AS a\n" +
                "LEFT OUTER JOIN (\n" +
                "  SELECT id, name \n" +
                "  FROM sut\n" +
                ") AS b\n" +
                "ON a.id = b.id";
        SqlParser.Config config = SqlParser.config()
                .withParserFactory(FlinkSqlParserImpl.FACTORY)
                .withQuoting(Quoting.BACK_TICK)
                .withUnquotedCasing(Casing.TO_LOWER)
                .withQuotedCasing(Casing.UNCHANGED)
                .withConformance(FlinkSqlConformance.DEFAULT);

        // 创建解析器
        SqlParser sqlParser = SqlParser.create(sql, config);
        // 生成 AST 语法树
        SqlNode sqlNode;
        try {
            sqlNode = sqlParser.parseStmt();
            System.out.println(sqlNode);
        } catch (SqlParseException e) {
            throw new RuntimeException("使用 Calcite 进行语法分析发生了异常", e);
        }
    }
}
