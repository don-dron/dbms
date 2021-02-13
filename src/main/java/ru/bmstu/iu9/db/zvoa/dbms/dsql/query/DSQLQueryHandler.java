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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.query;

import ru.bmstu.iu9.db.zvoa.dbms.execute.CompilationError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IExecutor;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

import javax.validation.constraints.NotNull;

/**
 * The type Query handler.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class DSQLQueryHandler extends AbstractDbHandler<Query, Query> {

    private final IExecutor executor;

    /**
     * Instantiates a new DSQL query handler.
     *
     * @param executor the executor
     */
    public DSQLQueryHandler(@NotNull IExecutor executor) {
        this.executor = executor;
    }

    @Override
    public Query execute(Query query) {
        DSQLQuery dsqlQuery = ((DSQLQuery) query);

        try {
            String result = executor.execute(dsqlQuery.getRequest());
            dsqlQuery.setResponse(result);
        } catch (CompilationError compilationError) {
            dsqlQuery.setResponse(compilationError.getMessage());
        } catch (RuntimeError runtimeError) {
            dsqlQuery.setResponse(runtimeError.getMessage());
        }

        return query;
    }
}
