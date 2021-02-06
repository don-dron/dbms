package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.translator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.DSQLProgram;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IProgram;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ASTNode;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.translator.ITranslator;

public class DSQLTranslator implements ITranslator {
    private final Logger logger = LoggerFactory.getLogger(DSQLTranslator.class);

    public IProgram translate(ASTNode astNode) {
        logger.debug("Start translate program.");
        return new DSQLProgram();
    }
}
