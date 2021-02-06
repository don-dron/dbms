package ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.query.DSQLQuery;
import ru.bmstu.iu9.db.zvoa.dbms.io.ResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

/**
 * The type Http response handler.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class HttpResponseHandler extends ResponseHandler<HttpResponse> {
    @Override
    public HttpResponse execute(Query query) {
        DSQLQuery dsqlQuery = ((DSQLQuery) query);
        return new HttpResponse(
                dsqlQuery.getResponse(),
                dsqlQuery.getConnectionInformation());
    }
}
