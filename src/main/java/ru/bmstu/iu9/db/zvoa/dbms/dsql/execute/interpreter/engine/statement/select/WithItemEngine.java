package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select;

import net.sf.jsqlparser.statement.select.WithItem;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.IEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

public class WithItemEngine extends DSQLEngine<WithItem> {
    public WithItemEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    @Override
    public void execute(WithItem withItem) throws RuntimeError {
        throw new RuntimeError("Unsupported " + withItem);
    }
}
