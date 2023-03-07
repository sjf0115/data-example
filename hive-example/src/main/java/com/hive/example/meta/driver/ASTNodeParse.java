package com.hive.example.meta.driver;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

/**
 * 功能：打印 Hive 命令对应的抽象语法树
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/1/8 下午4:49
 */
public class ASTNodeParse {
    public static void main(String[] args) {
        try {
            String command = "SELECT COUNT(*) AS num FROM behavior WHERE uid LIKE 'a%'";
            ParseDriver pd = new ParseDriver();
            ASTNode tree = pd.parse(command);
            System.out.println(tree.dump());
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
