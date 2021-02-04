package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import ru.bmstu.iu9.db.zvoa.dbms.query.SqlQuery;

/**
 * The type Dsql query.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class DSQLQuery extends SqlQuery {
    private final Connection connection;

    /**
     * Instantiates a new Dsql query.
     *
     * @param connection the connection
     */
    public DSQLQuery(Connection connection) {
        this.connection = connection;
    }

    /**
     * Gets connection information.
     *
     * @return the connection information
     */
    public Connection getConnectionInformation() {
        return connection;
    }
}
