package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

/**
 * The type Http response.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class HttpResponse {
    private final Connection connection;
    private final String response;

    /**
     * Instantiates a new Http response.
     *
     * @param response   the response
     * @param connection the connection
     */
    public HttpResponse(String response, Connection connection) {
        this.response = response;
        this.connection = connection;
    }

    /**
     * Gets connection.
     *
     * @return the connection
     */
    public Connection getConnection() {
        return connection;
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
