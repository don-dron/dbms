package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import ru.bmstu.iu9.db.zvoa.dbms.execute.CompilationError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IExecutor;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class JSQLInterpreter implements IExecutor {

    @Override
    public String execute(String string) throws CompilationError, RuntimeError {
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(string);
            BlockingDeque<Exception> runtimeExceptions = new LinkedBlockingDeque<>();
            List<Thread> executors = new ArrayList<>();

            BlockingDeque<Result> results = new LinkedBlockingDeque<>();

            for (Statement statement : statements.getStatements()) {
                executors.add(new Thread(() -> {
                    try {
                        String result = executeStatement(statement);
                        results.addLast(new Result(statement, result));
                    } catch (RuntimeError runtimeError) {
                        runtimeExceptions.add(runtimeError);
                    }
                }));
            }

            for (Thread thread : executors) {
                thread.start();
            }

            for (Thread thread : executors) {
                thread.join();
            }

            return results.stream().map(Result::toString).collect(Collectors.joining(", "));
        } catch (JSQLParserException e) {
            throw new CompilationError(e.getMessage());
        } catch (InterruptedException e) {
            throw new RuntimeError(e.getMessage());
        }
    }

    private String executeStatement(Statement statement) throws RuntimeError {
        return statement.toString();
    }

    private class Result {
        private final Statement statement;
        private final String result;

        public Result(Statement statement, String result) {
            this.statement = statement;
            this.result = result;
        }

        public Statement getStatement() {
            return statement;
        }

        public String getResult() {
            return result;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "statement=" + statement +
                    ", result='" + result + '\'' +
                    '}';
        }
    }
}
