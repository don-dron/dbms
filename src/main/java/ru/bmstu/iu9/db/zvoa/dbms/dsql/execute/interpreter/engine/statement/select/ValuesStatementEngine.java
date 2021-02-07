package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select;

import net.sf.jsqlparser.statement.values.ValuesStatement;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.IEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

public class ValuesStatementEngine extends DSQLEngine<ValuesStatement> {
    public ValuesStatementEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    @Override
    public void execute(ValuesStatement valuesStatement) throws RuntimeError {
        throw new RuntimeError("Unsupported " + valuesStatement);
    }
}
