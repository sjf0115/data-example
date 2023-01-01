grammar LabeledExpr;

//@header {
//package com.antlr.example.labeledExpr;
//}

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

// 命名
MUL     :   '*' ;
DIV     :   '/' ;
ADD     :   '+' ;
SUB     :   '-' ;

ID      :   [a-zA-Z]+ ;
INT     :   [0-9]+ ;
NEWLINE :   '\r'? '\n' ;
WS      :   [ \t]+ -> skip ;