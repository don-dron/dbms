package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import ru.bmstu.iu9.db.zvoa.dbms.io.ResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

public class HttpResponseHandler extends ResponseHandler<HttpResponse> {
    @Override
    public HttpResponse execute(Query o) {
        return new HttpResponse("Response",
                ((DSQLQuery) o).getConnectionInformation());
    }
}
