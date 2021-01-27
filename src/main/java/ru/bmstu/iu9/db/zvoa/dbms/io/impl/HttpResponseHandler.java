package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import java.net.http.HttpResponse;

import ru.bmstu.iu9.db.zvoa.dbms.io.ResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponse;

public class HttpResponseHandler  extends ResponseHandler<String> {
    @Override
    public String execute(QueryResponse o) {
        return "response";
    }
}
