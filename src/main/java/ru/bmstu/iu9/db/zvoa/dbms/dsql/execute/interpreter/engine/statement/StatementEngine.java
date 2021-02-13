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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.delete.DeleteEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.insert.InsertEngine;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select.SelectEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

import java.util.stream.Collectors;

public class StatementEngine extends DSQLEngine<Statement> {
    public StatementEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public Object execute(Statement statement) throws RuntimeError {
        if (statement instanceof Select) {
            return new SelectEngine(dataStorage).execute((Select) statement)
                    .stream()
                    .map(row -> row.getValues().stream().map(Object::toString).collect(Collectors.joining(", ")))
                    .collect(Collectors.joining("\n"));
        } else if (statement instanceof Insert) {
            return new InsertEngine(dataStorage).execute((Insert) statement);
        } else if (statement instanceof Delete) {
            return new DeleteEngine(dataStorage).execute((Delete) statement);
        } else {
            throw new RuntimeError("Not supported statement type " + statement);
        }
    }
}
