package ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http;

import javax.validation.constraints.NotNull;

/**
 * The type Http request.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class HttpRequest {
    @NotNull
    private final String id;

    @NotNull
    private final ConnectionInformation connectionInformation;

    @NotNull
    private final String request;

    private final long timestamp;

    /**
     * Instantiates a new Http request.
     *
     * @param id                    the id
     * @param request               the request
     * @param connectionInformation the connection
     * @param timestamp             the timestamp
     */
    public HttpRequest(@NotNull String id,
                       @NotNull String request,
                       @NotNull ConnectionInformation connectionInformation,
                       long timestamp) {
        this.id = id;
        this.request = request;
        this.connectionInformation = connectionInformation;
        this.timestamp = timestamp;
    }

    /**
     * Gets id.
     *
     * @return the id
     */
    @NotNull
    public String getId() {
        return id;
    }

    /**
     * Gets connection.
     *
     * @return the connection
     */
    @NotNull
    public ConnectionInformation getConnectionInformation() {
        return connectionInformation;
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
    @NotNull
    public String getRequest() {
        return request;
    }
}
