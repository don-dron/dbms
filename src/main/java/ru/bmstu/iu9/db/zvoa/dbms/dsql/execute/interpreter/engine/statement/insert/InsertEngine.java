package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.insert;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.InsertSettings;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InsertEngine extends DSQLEngine<Insert> {
    public InsertEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public void execute(Insert insert) throws RuntimeError {
        Table table = insert.getTable();
        ItemsList itemsList = insert.getItemsList();

        try {
            List<List<Object>> values = ((ExpressionList) itemsList)
                    .getExpressions()
                    .stream()
                    .map(expression -> {
                        return ((LongValue) expression).getValue();
                    })
                    .map(value -> Arrays.asList((Object) value))
                    .collect(Collectors.toList());

            dataStorage.insertRows(
                    InsertSettings.Builder.newBuilder()
                            .setSchemaName(table.getSchemaName())
                            .setTableName(table.getName())
                            .setRows(values)
                            .build());
        } catch (DataStorageException dataStorageException) {
            throw new RuntimeError(dataStorageException.getMessage());
        }
    }
}
