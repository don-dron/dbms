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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.SelectSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;

import java.util.List;

public class PlainSelectEngine extends DSQLEngine<PlainSelect> {
    public PlainSelectEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public List<Row> execute(PlainSelect plainSelect) throws RuntimeError {
        FromItem fromItem = plainSelect.getFromItem();
        Table table = (Table) fromItem;
        try {
            List<Row> rowList =
                    dataStorage.selectRows(SelectSettings.Builder.newBuilder()
                            .setSchemaName(table.getSchemaName())
                            .setTableName(table.getName())
                            .build());
            return rowList;
        } catch (DataStorageException dataStorageException) {
            throw new RuntimeError(dataStorageException.getMessage());
        }
    }
}
