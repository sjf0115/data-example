grammar AlternativeLabels;

stat:   'return' e ';'     # Return
    |   'break'    ';'     # Break
    ;

e   :   e '*' e      # BinaryOp
    |   e '+' e      # BinaryOp
    |   INT  # Int
    ;

INT :   [0-9]+ ;