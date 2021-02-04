package ru.bmstu.iu9.db.zvoa.dbms.io;

import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

/**
 * The type Response handler.
 *
 * @param <R> the type parameter
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public abstract class ResponseHandler<R> implements IDbHandler<Query, R> {
    @Override
    public abstract R execute(Query o);
}
