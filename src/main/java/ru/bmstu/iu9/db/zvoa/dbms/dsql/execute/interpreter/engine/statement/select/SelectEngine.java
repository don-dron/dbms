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

import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;

import java.util.List;

public class SelectEngine extends DSQLEngine<Select> {
    public SelectEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public List<Row> execute(Select select) throws RuntimeError {
        SelectBody selectBody = select.getSelectBody();
        List<WithItem> withItems = select.getWithItemsList();

        if (withItems != null) {
            throw new RuntimeError("Unsupported \"with\" blocks");
        }

        if (selectBody instanceof PlainSelect) {
            return new PlainSelectEngine(dataStorage).execute((PlainSelect) selectBody);
        } else if (selectBody instanceof ValuesStatement) {
            return new ValuesStatementEngine(dataStorage).execute((ValuesStatement) selectBody);
        } else if (selectBody instanceof SetOperationList) {
            return new SetOperationListEngine(dataStorage).execute((SetOperationList) selectBody);
        } else if (selectBody instanceof WithItem) {
            return new WithItemEngine(dataStorage).execute((WithItem) selectBody);
        } else {
            throw new RuntimeError("Undefined select body " + selectBody);
        }
    }
}
