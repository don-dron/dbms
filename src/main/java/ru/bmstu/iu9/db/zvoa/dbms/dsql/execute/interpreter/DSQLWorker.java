package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter;

import net.sf.jsqlparser.statement.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.StatementEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

public class DSQLWorker extends Thread {
    private final Logger logger = LoggerFactory.getLogger(DSQLWorker.class);
    private Statement statement;
    private DataStorage dataStorage;
    private DSQLResult result;

    public DSQLWorker(Statement statement, DataStorage dataStorage) {
        this.statement = statement;
        this.dataStorage = dataStorage;
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
        new StatementEngine(dataStorage).execute(statement);
        return "res";
    }
}
