package ru.bmstu.iu9.db.zvoa.dbms.io;

import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

/**
 * The type Request handler.
 *
 * @param <T> the type parameter
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public abstract class RequestHandler<T> implements IDbHandler<T, Query> {
    @Override
    public abstract Query execute(T o);
}
