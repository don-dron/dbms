package ru.bmstu.iu9.db.zvoa.dbms.query;

import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbHandler;

public class QueryHandler extends AbstractDbHandler<QueryRequest, QueryResponse> {
    @Override
    public QueryResponse execute(QueryRequest queryRequest) {
        return new QueryResponse();
    }
}
