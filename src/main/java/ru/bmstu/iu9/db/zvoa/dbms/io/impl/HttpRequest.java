package ru.bmstu.iu9.db.zvoa.dbms.io.impl;


public class HttpRequest {
    private final Connection connection;
    private final String request;
    private final long timestamp;

    public HttpRequest(String request, Connection connection, long timestamp) {
        this.request = request;
        this.connection = connection;
        this.timestamp = timestamp;
    }

    public Connection getConnection() {
        return connection;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getRequest() {
        return request;
    }
}
