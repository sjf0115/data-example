package com.antlr.example.calculator;

import com.antlr.example.labeledExpr.LabeledExprParser;

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

    // expr NEWLINE
    @Override
    public Integer visitPrintExpr(CalculatorParser.PrintExprContext ctx) {
        String text = ctx.expr().getText();
        // 计算 expr 子节点的值
        Integer value = visit(ctx.expr());
        System.out.println(text + "=" + value);
        // 返回值无所谓
        return value;
    }

    // ID '=' expr NEWLINE
    @Override
    public Integer visitAssign(CalculatorParser.AssignContext ctx) {
        // id 在 = 的右侧
        String id = ctx.ID().getText();
        // 计算右侧表达式的值
        Integer value = visit(ctx.expr());
        // 将映射关系存入 memory 变量中
        memory.put(id, value);
        return value;
    }

    @Override
    public Integer visitBlank(CalculatorParser.BlankContext ctx) {
        return 0;
    }

    @Override
    public Integer visitParens(CalculatorParser.ParensContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public Integer visitMulDiv(CalculatorParser.MulDivContext ctx) {
        Integer left = visit(ctx.expr(0));
        Integer right = visit(ctx.expr(1));
        if (ctx.op.getType() == LabeledExprParser.MUL) {
            return left * right;
        } else {
            return left / right;
        }
    }

    @Override
    public Integer visitAddSub(CalculatorParser.AddSubContext ctx) {
        Integer left = visit(ctx.expr(0));
        Integer right = visit(ctx.expr(1));
        if (ctx.op.getType() == LabeledExprParser.ADD) {
            return left + right;
        } else {
            return left - right;
        }
    }

    // ID
    @Override
    public Integer visitId(CalculatorParser.IdContext ctx) {
        String id = ctx.ID().getText();
        if (memory.containsKey(id)) {
            return memory.get(id);
        }
        return 0;
    }

    // INT
    @Override
    public Integer visitInt(CalculatorParser.IntContext ctx) {
        // 计算 INT 的值
        String value = ctx.INT().getText();
        return Integer.parseInt(value);
    }
}
