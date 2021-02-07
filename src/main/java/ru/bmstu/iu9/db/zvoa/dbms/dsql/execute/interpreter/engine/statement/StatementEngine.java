package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.IEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.delete.DeleteEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.insert.InsertEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select.SelectEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

public class StatementEngine extends DSQLEngine<Statement> {
    public StatementEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public void execute(Statement statement) throws RuntimeError {
        if (statement instanceof Select) {
            new SelectEngine(dataStorage).execute((Select) statement);
        } else if (statement instanceof Insert) {
            new InsertEngine(dataStorage).execute((Insert) statement);
        } else if (statement instanceof Delete) {
            new DeleteEngine(dataStorage).execute((Delete) statement);
        } else {
            throw new RuntimeError("Not supported statement type " + statement);
        }
    }
}
