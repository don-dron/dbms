package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.optimizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.DSQLProgram;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IProgram;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.optimizer.IOptimizer;

public class JSQLOptimizer implements IOptimizer {
    private final Logger logger = LoggerFactory.getLogger(JSQLOptimizer.class);

    @Override
    public IProgram optimize(IProgram program) {
        logger.debug("Start optimize program.");
        return new DSQLProgram();
    }
}