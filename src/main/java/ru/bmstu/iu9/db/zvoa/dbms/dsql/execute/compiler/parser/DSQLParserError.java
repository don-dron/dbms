
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.parser;

import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ParserError;

public class DSQLParserError extends ParserError {

    private static final long serialVersionUID = 1L;

    public DSQLParserError(String message) {
        super(message);
    }

    public DSQLParserError(String message, Throwable e) {
        super(message, e);
    }

    public DSQLParserError(String message, int line, int col) {
        super(message);
    }

    public DSQLParserError(Throwable ex, String ksql) {
        super("parse error. detail message is :\n" + ex.getMessage() + "\nsource sql is : \n" + ksql, ex);
    }
}
