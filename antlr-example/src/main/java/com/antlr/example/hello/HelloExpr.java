package com.antlr.example.hello;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.IOException;

/**
 * 功能：Hello xxx
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/1/1 下午3:53
 */
public class HelloExpr {
    public static void main(String[] args) throws IOException {
        String input = "hello antlr";

        // 1. 新建一个 CharStream 从字符串中读取数据
        CharStream charStream = CharStreams.fromString(input);
        // 新建一个 CharStream 从标准输入中读取数据
        // CharStream charStream = CharStreams.fromStream(System.in);

        // 2. 创建词法分析器
        HelloLexer lexer = new HelloLexer(charStream);

        // 3. 创建词法符号缓冲区对象
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        // 4. 创建语法分析器
        HelloParser parser = new HelloParser(tokens);

        // 5. 针对 prog 语法规则 开始语法分析
        HelloParser.RContext tree = parser.r();

        //5. 打印 语法分析树
        System.out.println(tree.toStringTree(parser));
    }
}
