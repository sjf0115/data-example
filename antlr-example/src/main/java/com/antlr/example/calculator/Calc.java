package com.antlr.example.calculator;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * 功能：计算器驱动类
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/1/2 下午5:59
 */
public class Calc {
    public static void main(String[] args) {
        String input = "1 + 2 * 3 + (3 + 1) * 2\n";

        // 1. 新建一个 CharStream 从字符串中读取数据
        CharStream charStream = CharStreams.fromString(input);
        // 新建一个 CharStream 从标准输入中读取数据
        // CharStream charStream = CharStreams.fromStream(System.in);

        // 2. 创建词法分析器
        CalculatorLexer lexer = new CalculatorLexer(charStream);

        // 3. 创建词法符号缓冲区对象
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        // 4. 创建语法分析器
        CalculatorParser parser = new CalculatorParser(tokens);

        // 5. 针对 prog 语法规则 开始语法分析
        CalculatorParser.ProgContext tree = parser.prog();

        //5. 打印 语法分析树
        System.out.println(tree.toStringTree(parser));

        // 6. 访问器模式执行
        CalcVisitor visitor = new CalcVisitor();
        visitor.visit(tree);
    }
}
