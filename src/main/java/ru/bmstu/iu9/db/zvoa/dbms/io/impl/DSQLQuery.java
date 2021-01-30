package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import ru.bmstu.iu9.db.zvoa.dbms.query.SqlQuery;

public class DSQLQuery extends SqlQuery {
    private final Connection connection;

    public DSQLQuery(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnectionInformation() {
        return connection;
    }
}
