package com.antlr.example.labeledExpr;

import java.util.HashMap;

/**
 * 功能：自定义访问器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/1/1 下午4:15
 */
public class EvalVisitor extends LabeledExprBaseVisitor<Integer> {
    // 计数器内存 存放变量名和变量值的关系
    private HashMap<String, Integer> memory = new HashMap<>();

    // ID '=' expr NEWLINE
    @Override
    public Integer visitAssign(LabeledExprParser.AssignContext ctx) {
        // id 在 = 的右侧
        String id = ctx.ID().getText();
        // 计算右侧表达式的值
        Integer value = visit(ctx.expr());
        // 将映射关系存入 memory 变量中
        memory.put(id, value);
        return value;
    }

    // expr NEWLINE
    @Override
    public Integer visitPrintExpr(LabeledExprParser.PrintExprContext ctx) {
        String text = ctx.expr().getText();
        // 计算 expr 子节点的值
        Integer value = visit(ctx.expr());
        System.out.println(text + "=" + value);
        // 返回值无所谓
        return value;
    }

    // INT
    @Override
    public Integer visitInt(LabeledExprParser.IntContext ctx) {
        // 计算 INT 的值
        String value = ctx.INT().getText();
        return Integer.parseInt(value);
    }

    // ID
    @Override
    public Integer visitId(LabeledExprParser.IdContext ctx) {
        String id = ctx.ID().getText();
        if (memory.containsKey(id)) {
            return memory.get(id);
        }
        return 0;
    }

    @Override
    public Integer visitMulDiv(LabeledExprParser.MulDivContext ctx) {
        Integer left = visit(ctx.expr(0));
        Integer right = visit(ctx.expr(1));
        if (ctx.op.getType() == LabeledExprParser.MUL) {
            return left * right;
        } else {
            return left / right;
        }
    }

    @Override
    public Integer visitAddSub(LabeledExprParser.AddSubContext ctx) {
        Integer left = visit(ctx.expr(0));
        Integer right = visit(ctx.expr(1));
        if (ctx.op.getType() == LabeledExprParser.ADD) {
            return left + right;
        } else {
            return left - right;
        }
    }

    @Override
    public Integer visitParens(LabeledExprParser.ParensContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public Integer visitBlank(LabeledExprParser.BlankContext ctx) {
        return 0;
    }
}
