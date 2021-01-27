package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import java.net.http.HttpRequest;

import ru.bmstu.iu9.db.zvoa.dbms.io.RequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequest;

public class HttpRequestHandler extends RequestHandler<String> {
    @Override
    public QueryRequest execute(String o) {
        return new QueryRequest();
    }
}
