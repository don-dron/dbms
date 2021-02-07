package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select;

import net.sf.jsqlparser.statement.select.SetOperationList;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.IEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

public class SetOperationListEngine extends DSQLEngine<SetOperationList> {
    public SetOperationListEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    @Override
    public void execute(SetOperationList setOperationList) throws RuntimeError {
        throw new RuntimeError("Unsupported " + setOperationList);
    }
}
