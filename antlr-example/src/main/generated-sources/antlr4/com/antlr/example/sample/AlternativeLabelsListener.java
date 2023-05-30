// Generated from com/antlr/example/sample/AlternativeLabels.g4 by ANTLR 4.9.2
package com.antlr.example.sample;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link AlternativeLabelsParser}.
 */
public interface AlternativeLabelsListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by the {@code Return}
	 * labeled alternative in {@link AlternativeLabelsParser#stat}.
	 * @param ctx the parse tree
	 */
	void enterReturn(AlternativeLabelsParser.ReturnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Return}
	 * labeled alternative in {@link AlternativeLabelsParser#stat}.
	 * @param ctx the parse tree
	 */
	void exitReturn(AlternativeLabelsParser.ReturnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Break}
	 * labeled alternative in {@link AlternativeLabelsParser#stat}.
	 * @param ctx the parse tree
	 */
	void enterBreak(AlternativeLabelsParser.BreakContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Break}
	 * labeled alternative in {@link AlternativeLabelsParser#stat}.
	 * @param ctx the parse tree
	 */
	void exitBreak(AlternativeLabelsParser.BreakContext ctx);
	/**
	 * Enter a parse tree produced by the {@code Int}
	 * labeled alternative in {@link AlternativeLabelsParser#e}.
	 * @param ctx the parse tree
	 */
	void enterInt(AlternativeLabelsParser.IntContext ctx);
	/**
	 * Exit a parse tree produced by the {@code Int}
	 * labeled alternative in {@link AlternativeLabelsParser#e}.
	 * @param ctx the parse tree
	 */
	void exitInt(AlternativeLabelsParser.IntContext ctx);
	/**
	 * Enter a parse tree produced by the {@code BinaryOp}
	 * labeled alternative in {@link AlternativeLabelsParser#e}.
	 * @param ctx the parse tree
	 */
	void enterBinaryOp(AlternativeLabelsParser.BinaryOpContext ctx);
	/**
	 * Exit a parse tree produced by the {@code BinaryOp}
	 * labeled alternative in {@link AlternativeLabelsParser#e}.
	 * @param ctx the parse tree
	 */
	void exitBinaryOp(AlternativeLabelsParser.BinaryOpContext ctx);
}