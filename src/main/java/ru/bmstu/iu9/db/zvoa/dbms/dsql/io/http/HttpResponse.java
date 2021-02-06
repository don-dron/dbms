package ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http;

/**
 * The type Http response.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class HttpResponse {
    private final ConnectionInformation connectionInformation;
    private final String response;

    /**
     * Instantiates a new Http response.
     *
     * @param response   the response
     * @param connectionInformation the connection
     */
    public HttpResponse(String response, ConnectionInformation connectionInformation) {
        this.response = response;
        this.connectionInformation = connectionInformation;
    }

    /**
     * Gets connection.
     *
     * @return the connection
     */
    public ConnectionInformation getConnectionInformation() {
        return connectionInformation;
    }

    /**
     * Gets response.
     *
     * @return the response
     */
    public String getResponse() {
        return response;
    }
}
