package ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.query.DSQLQuery;
import ru.bmstu.iu9.db.zvoa.dbms.io.RequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

/**
 * The type Http request handler.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class HttpRequestHandler extends RequestHandler<HttpRequest> {
    @Override
    public Query execute(HttpRequest httpRequest) {
        return new DSQLQuery(httpRequest.getRequest(), httpRequest.getConnectionInformation());
    }
}
