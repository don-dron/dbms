package ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer;

import java.util.stream.Stream;

public interface ILexer {
    public Stream<IToken> lex(String code) throws LexerError;
}
