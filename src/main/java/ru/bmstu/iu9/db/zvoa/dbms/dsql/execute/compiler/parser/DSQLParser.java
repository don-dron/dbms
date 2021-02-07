package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.IToken;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ASTNode;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.IParser;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ParserError;

import java.util.List;

public class DSQLParser implements IParser {
    private final Logger logger = LoggerFactory.getLogger(DSQLParser.class);

    public ASTNode parse(List<IToken> tokenStream) throws ParserError {
        logger.debug("Start parse program.");
        //        TODO Parser
        return new DSQLNode();
    }
}
