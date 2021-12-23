/** Grammars always start with a grammar header. This grammar is called
 *  ArrayInit and must match the filename: ArrayInit.g4
 */
grammar CoronaData;

/** A rule called init that matches comma-separated values between {...}. */
data  : 'var data = {' (entry ',' )  * '}' ;

entry : QUOTE ARS QUOTE ':' '{' pair (',' pair)* '}'  ;

pair:  NAME ':' VALUE ;

NAME:  [a-z_]+ ;
WS  :   [ \t\r\n]+ -> skip ; // Define whitespace rule, toss it out
QUOTE:   [\'\"] ;
ARS:   '010' [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9] ;
VALUE:  [0-9]+ '.'? [0-9]* ;

