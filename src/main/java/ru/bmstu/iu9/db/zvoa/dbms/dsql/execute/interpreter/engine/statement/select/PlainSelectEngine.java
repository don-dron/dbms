package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.SelectSettings;

import java.util.List;

public class PlainSelectEngine extends DSQLEngine<PlainSelect> {
    public PlainSelectEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public void execute(PlainSelect plainSelect) throws RuntimeError {
        FromItem fromItem = plainSelect.getFromItem();
        Table table = (Table) fromItem;
        try {
            List<ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row> rowList =
                    dataStorage.selectRows(SelectSettings.Builder.newBuilder()
                            .setSchemaName(table.getSchemaName())
                            .setTableName(table.getName())
                            .build());
            System.out.println("ROWS COUNT: " + rowList.size() + " " + rowList);
        } catch (DataStorageException dataStorageException) {
            throw new RuntimeError(dataStorageException.getMessage());
        }
    }
}
