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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.execute.CompilationError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IExecutor;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

public class JSQLInterpreter implements IExecutor {
    private final Logger logger = LoggerFactory.getLogger(JSQLInterpreter.class);
    private final DataStorage dbmsStorage;

    public JSQLInterpreter(DataStorage storage) {
        this.dbmsStorage = storage;
    }

    @Override
    public String execute(String string) throws CompilationError, RuntimeError {
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(string);
            List<DSQLWorker> executors = new ArrayList<>();
            BlockingDeque<DSQLResult> results = new LinkedBlockingDeque<>();

            for (Statement statement : statements.getStatements()) {
                executors.add(new DSQLWorker(statement, dbmsStorage));
            }

            for (DSQLWorker thread : executors) {
                try {
                    thread.start();
                    thread.join();
                    results.addLast(thread.getResult());
                } catch (InterruptedException e) {
                    throw new RuntimeError(e.getMessage());
                }
            }

            return results.stream().map(DSQLResult::getResult).collect(Collectors.joining(", "));
        } catch (JSQLParserException e) {
            throw new CompilationError(e.getMessage());
        }
    }
}
