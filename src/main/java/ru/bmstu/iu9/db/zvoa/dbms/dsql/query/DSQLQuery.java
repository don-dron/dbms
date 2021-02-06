package ru.bmstu.iu9.db.zvoa.dbms.dsql.query;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.ConnectionInformation;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

import javax.validation.constraints.NotNull;

/**
 * The DSQL Query.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class DSQLQuery implements Query {
    @NotNull
    private final ConnectionInformation connectionInformation;

    @NotNull
    private final String request;

    private String response;

    /**
     * Instantiates a new DSQL query.
     *
     * @param sqlCode               the sql code
     * @param connectionInformation the connection
     */
    public DSQLQuery(@NotNull String sqlCode, @NotNull ConnectionInformation connectionInformation) {
        this.request = sqlCode;
        this.connectionInformation = connectionInformation;
    }

    /**
     * Gets request.
     *
     * @return the request
     */
    @NotNull
    public String getRequest() {
        return request;
    }

    /**
     * Gets connection information.
     *
     * @return the connection information
     */
    @NotNull
    public ConnectionInformation getConnectionInformation() {
        return connectionInformation;
    }

    /**
     * Sets response.
     *
     * @param response the response
     */
    public void setResponse(@NotNull String response) {
        this.response = response;
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
