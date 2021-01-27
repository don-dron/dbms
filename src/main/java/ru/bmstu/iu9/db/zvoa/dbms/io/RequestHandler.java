package ru.bmstu.iu9.db.zvoa.dbms.io;

import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequest;

public abstract class RequestHandler<T> implements IDbHandler<T, QueryRequest> {
    @Override
    public abstract QueryRequest execute(T o);
}
