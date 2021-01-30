package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import ru.bmstu.iu9.db.zvoa.dbms.io.RequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

public class HttpRequestHandler extends RequestHandler<HttpRequest> {
    @Override
    public Query execute(HttpRequest o) {
        return new DSQLQuery(o.getConnection());
    }
}
