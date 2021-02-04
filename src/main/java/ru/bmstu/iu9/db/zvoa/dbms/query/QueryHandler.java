package ru.bmstu.iu9.db.zvoa.dbms.query;

import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

/**
 * The type Query handler.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class QueryHandler extends AbstractDbHandler<Query, Query> {
    @Override
    public Query execute(Query queryRequest) {
        return queryRequest;
    }
}
