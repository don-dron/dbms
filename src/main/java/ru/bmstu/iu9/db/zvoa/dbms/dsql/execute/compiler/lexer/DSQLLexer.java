package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.lexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.ILexer;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.IToken;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.LexerError;

import java.util.ArrayList;
import java.util.List;

public class DSQLLexer implements ILexer {
    private final Logger logger = LoggerFactory.getLogger(DSQLLexer.class);

    @Override
    public List<IToken> lex(String code) throws LexerError {
        logger.debug("Start lex program.");
//        List<IToken> tokens = new DSQLIdentifier(code);
//        logger.debug(tokens.stream().map(IToken::toString).collect(Collectors.joining(", ")));
//        return tokens;
        return new ArrayList<>();
    }
}
