// Generated from com/antlr/example/sample/AlternativeLabels.g4 by ANTLR 4.9.2
package com.antlr.example.sample;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link AlternativeLabelsParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface AlternativeLabelsVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by the {@code Return}
	 * labeled alternative in {@link AlternativeLabelsParser#stat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReturn(AlternativeLabelsParser.ReturnContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Break}
	 * labeled alternative in {@link AlternativeLabelsParser#stat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBreak(AlternativeLabelsParser.BreakContext ctx);
	/**
	 * Visit a parse tree produced by the {@code Int}
	 * labeled alternative in {@link AlternativeLabelsParser#e}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInt(AlternativeLabelsParser.IntContext ctx);
	/**
	 * Visit a parse tree produced by the {@code BinaryOp}
	 * labeled alternative in {@link AlternativeLabelsParser#e}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBinaryOp(AlternativeLabelsParser.BinaryOpContext ctx);
}