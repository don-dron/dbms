package ru.bmstu.iu9.db.zvoa.dbms.io;

import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponse;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

public abstract class ResponseHandler<R> implements IDbHandler<QueryResponse, R> {
    @Override
    public abstract R execute(QueryResponse o);
}
