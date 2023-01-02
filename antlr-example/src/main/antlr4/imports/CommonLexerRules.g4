lexer grammar CommonLexerRules; // 定义一个名为 CommonLexerRules 的词法分析器语法

ID      :   [a-zA-Z]+ ;           // 匹配大小写字母组成的标识符
INT     :   [0-9]+ ;              // 匹配整数组成的标识符
NEWLINE :   '\r'? '\n' ;          // 新行
WS      :   [ \t\r\n]+ -> skip ;  // 忽略空格、Tab、换行以及\r
EQL     :   '=' ;                 // 等于
MUL     :   '*' ;                 // 乘法
DIV     :   '/' ;                 // 除法
ADD     :   '+' ;                 // 加法
SUB     :   '-' ;                 // 减法