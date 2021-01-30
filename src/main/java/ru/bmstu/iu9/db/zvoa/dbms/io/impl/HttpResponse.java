package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

public class HttpResponse {
    private final Connection connection;
    private final String response;

    public HttpResponse(String response, Connection connection) {
        this.response = response;
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    public String getResponse() {
        return response;
    }
}
