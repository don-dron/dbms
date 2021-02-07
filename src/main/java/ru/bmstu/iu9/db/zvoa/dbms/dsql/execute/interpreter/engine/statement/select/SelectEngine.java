package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select;

import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.IEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

import java.util.List;

public class SelectEngine extends DSQLEngine<Select> {
    public SelectEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public void execute(Select select) throws RuntimeError {
        SelectBody selectBody = select.getSelectBody();
        List<WithItem> withItems = select.getWithItemsList();

        if (withItems != null) {
            throw new RuntimeError("Unsupported \"with\" blocks");
        }

        if (selectBody instanceof PlainSelect) {
            new PlainSelectEngine(dataStorage).execute((PlainSelect) selectBody);
        } else if (selectBody instanceof ValuesStatement) {
            new ValuesStatementEngine(dataStorage).execute((ValuesStatement) selectBody);
        } else if (selectBody instanceof SetOperationList) {
            new SetOperationListEngine(dataStorage).execute((SetOperationList) selectBody);
        } else if (selectBody instanceof WithItem) {
            new WithItemEngine(dataStorage).execute((WithItem) selectBody);
        } else {
            throw new RuntimeError("Undefined select body " + selectBody);
        }
    }
}
