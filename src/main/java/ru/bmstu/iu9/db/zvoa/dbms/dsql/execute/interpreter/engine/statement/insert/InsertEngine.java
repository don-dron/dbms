package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.insert;

import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.IEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

public class InsertEngine extends DSQLEngine<Insert> {
    public InsertEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public void execute(Insert insert) throws RuntimeError {

    }
}
