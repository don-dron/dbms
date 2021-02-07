package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter;

import net.sf.jsqlparser.statement.Statement;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;

public class DSQLResult {
    private final Statement statement;
    private final String result;

    public DSQLResult(Statement statement, String result) {
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