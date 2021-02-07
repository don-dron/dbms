package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.semanter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.parser.DSQLParser;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ASTNode;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.semanter.ISemanter;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.semanter.SemanticError;

public class DSQLSemanter implements ISemanter {
    private final Logger logger = LoggerFactory.getLogger(DSQLSemanter.class);

    @Override
    public void checkSemantic(ASTNode astNode) throws SemanticError {
        logger.debug("Start semantic checking program.");
        //        TODO Semanter
    }
}
