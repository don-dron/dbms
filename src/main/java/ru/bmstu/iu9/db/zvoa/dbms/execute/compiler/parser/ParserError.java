package ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser;

/**
 * The type Parser error.
 */
public class ParserError extends Exception {
    /**
     * Instantiates a new Parser error.
     */
    public ParserError(String message) {
        super(message);
    }

    /**
     * Instantiates a new Parser error.
     */
    public ParserError(String message, Throwable throwable) {
        super(message, throwable);
    }
}
