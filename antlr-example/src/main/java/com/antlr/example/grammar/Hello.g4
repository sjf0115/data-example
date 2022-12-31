grammar Hello; // 定义一个名为 Hello 的语法
r : 'hello' ID ; // 定义一个语法规则：匹配一个关键词 hello 和一个紧随其后的标识符
ID : [a-z]+ ; // 匹配小写字母组成的标识符
WS: [ \t\r\n]+ -> skip; // 忽略空格、Tab、换行以及\r