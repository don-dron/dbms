package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.delete;

import net.sf.jsqlparser.statement.delete.Delete;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

public class DeleteEngine extends DSQLEngine<Delete> {
    public DeleteEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public void execute(Delete delete) throws RuntimeError {

    }
}
