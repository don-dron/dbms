package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.execute.CompilationError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IExecutor;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class JSQLInterpreter implements IExecutor {
    private final Logger logger = LoggerFactory.getLogger(JSQLInterpreter.class);
    private final DataStorage dbmsStorage;

    public JSQLInterpreter(DataStorage storage) {
        this.dbmsStorage = storage;
    }

    @Override
    public String execute(String string) throws CompilationError, RuntimeError {
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(string);
            List<DSQLWorker> executors = new ArrayList<>();
            BlockingDeque<DSQLResult> results = new LinkedBlockingDeque<>();

            for (Statement statement : statements.getStatements()) {
                executors.add(new DSQLWorker(statement, dbmsStorage));
            }

            for (DSQLWorker thread : executors) {
                thread.start();
            }

            for (DSQLWorker thread : executors) {
                try {
                    thread.join();
                    results.addLast(thread.getResult());
                } catch (InterruptedException e) {
                    throw new RuntimeError(e.getMessage());
                }
            }

            return results.stream().map(DSQLResult::toString).collect(Collectors.joining(", "));
        } catch (JSQLParserException e) {
            throw new CompilationError(e.getMessage());
        }
    }
}
