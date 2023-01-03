package com.antlr.example.calculator;


import java.util.HashMap;

/**
 * 功能：自定义访问器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/1/2 下午6:04
 */
public class CalcVisitor extends CalculatorBaseVisitor<Integer> {
    // 计数器内存 存放变量名和变量值的关系
    private HashMap<String, Integer> memory = new HashMap<>();

    // expr NEWLINE 备选分支 printExpr 标签
    @Override
    public Integer visitPrintExpr(CalculatorParser.PrintExprContext ctx) {
        // 表达式
        String text = ctx.expr().getText();
        // 计算表达式的值
        Integer value = visit(ctx.expr());
        System.out.println(text + "=" + value);
        // 返回值无所谓
        return value;
    }

    // ID '=' expr NEWLINE 备选分支 assign 标签
    @Override
    public Integer visitAssign(CalculatorParser.AssignContext ctx) {
        // 获取 ID 值
        String id = ctx.ID().getText();
        // 计算右侧表达式的值
        Integer value = visit(ctx.expr());
        // 由于是赋值语句 将映射关系存入 memory 变量中
        memory.put(id, value);
        return value;
    }

    // NEWLINE 备选分支 blank 标签
    @Override
    public Integer visitBlank(CalculatorParser.BlankContext ctx) {
        return 0;
    }

    // expr op=('*'|'/') expr 备选分支 MulDiv
    @Override
    public Integer visitMulDiv(CalculatorParser.MulDivContext ctx) {
        // 左侧的数
        Integer left = visit(ctx.expr(0));
        // 右侧的数
        Integer right = visit(ctx.expr(1));
        // 运算
        if (ctx.op.getType() == CalculatorParser.MUL) {
            return left * right;
        } else {
            return left / right;
        }
    }

    // expr op=('+'|'-') expr 备选分支 AddSub
    @Override
    public Integer visitAddSub(CalculatorParser.AddSubContext ctx) {
        // 左侧的数
        Integer left = visit(ctx.expr(0));
        // 右侧的数
        Integer right = visit(ctx.expr(1));
        // 运算
        if (ctx.op.getType() == CalculatorParser.ADD) {
            return left + right;
        } else {
            return left - right;
        }
    }

    // INT 备选分支 int
    @Override
    public Integer visitInt(CalculatorParser.IntContext ctx) {
        // 计算 INT 的值
        String value = ctx.INT().getText();
        return Integer.parseInt(value);
    }

    // ID 备选分支 id
    @Override
    public Integer visitId(CalculatorParser.IdContext ctx) {
        // 获取 ID 变量
        String id = ctx.ID().getText();
        // 获取 ID 变量对应的值
        if (memory.containsKey(id)) {
            // 是否是赋值语句
            return memory.get(id);
        }
        return 0;
    }

    // '(' expr ')' 备选分支 parens
    @Override
    public Integer visitParens(CalculatorParser.ParensContext ctx) {
        return visit(ctx.expr());
    }
}
