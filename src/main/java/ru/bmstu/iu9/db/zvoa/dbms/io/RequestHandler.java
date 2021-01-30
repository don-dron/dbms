package ru.bmstu.iu9.db.zvoa.dbms.io;

import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

public abstract class RequestHandler<T> implements IDbHandler<T, Query> {
    @Override
    public abstract Query execute(T o);
}
