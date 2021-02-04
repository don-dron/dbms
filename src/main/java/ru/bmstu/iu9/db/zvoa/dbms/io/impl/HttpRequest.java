package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

/**
 * The type Http request.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class HttpRequest {
    private final Connection connection;
    private final String request;
    private final long timestamp;

    /**
     * Instantiates a new Http request.
     *
     * @param request    the request
     * @param connection the connection
     * @param timestamp  the timestamp
     */
    public HttpRequest(String request, Connection connection, long timestamp) {
        this.request = request;
        this.connection = connection;
        this.timestamp = timestamp;
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
     * Gets timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Gets request.
     *
     * @return the request
     */
    public String getRequest() {
        return request;
    }
}
