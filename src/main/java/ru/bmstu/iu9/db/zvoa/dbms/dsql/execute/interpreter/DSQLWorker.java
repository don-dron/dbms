package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter;

import net.sf.jsqlparser.statement.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.StatementEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;

public class DSQLWorker extends Thread {
    private final Logger logger = LoggerFactory.getLogger(DSQLWorker.class);
    private Statement statement;
    private DSQLResult result;

    public DSQLWorker(Statement statement) {
        this.statement = statement;
    }

    public DSQLResult getResult() {
        return result;
    }

    @Override
    public void run() {
        logger.debug("Start executing statement : " + statement);

        try {
            result = new DSQLResult(statement, executeStatement(statement));
        } catch (RuntimeError runtimeError) {
            result = new DSQLResult(statement, runtimeError.getMessage());
        }
    }

    private String executeStatement(Statement statement) throws RuntimeError {
        new StatementEngine().execute(statement);
        return "res";
    }
}
