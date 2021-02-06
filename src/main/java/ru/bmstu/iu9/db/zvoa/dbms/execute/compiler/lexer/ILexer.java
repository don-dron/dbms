package ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer;

import java.util.List;

public interface ILexer {
    public List<IToken> lex(String code) throws LexerError;
}
