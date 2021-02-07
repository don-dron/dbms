package ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser;

import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.IToken;

import java.util.List;
import java.util.stream.Stream;

public interface IParser {
    public ASTNode parse(List<IToken> tokenStream) throws ParserError;
}