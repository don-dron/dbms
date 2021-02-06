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
