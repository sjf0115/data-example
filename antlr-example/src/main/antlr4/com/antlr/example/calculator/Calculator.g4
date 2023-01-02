// 语法文件通常以 grammar 关键词开头 语法名称为 Calculator 所以文件名称也必须为 Calculator
grammar Calculator;

import CommonLexerRules ; // 引入 CommonLexerRules.g4 文件中的全部词法规则

prog:   stat+ ;

stat:   expr NEWLINE                # printExpr // 标签以 # 开头
    |   ID '=' expr NEWLINE         # assign
    |   NEWLINE                     # blank
    ;

expr:   expr op=('*'|'/') expr      # MulDiv
    |   expr op=('+'|'-') expr      # AddSub
    |   INT                         # int
    |   ID                          # id
    |   '(' expr ')'                # parens
    ;