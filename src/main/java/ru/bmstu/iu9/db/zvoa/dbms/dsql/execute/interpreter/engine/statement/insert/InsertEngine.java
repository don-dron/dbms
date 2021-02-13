/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
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
                    .map(expression -> ((LongValue) expression).getValue())
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
