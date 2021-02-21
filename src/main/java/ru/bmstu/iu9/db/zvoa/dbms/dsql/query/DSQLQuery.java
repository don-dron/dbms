/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.bmstu.iu9.db.zvoa.dbms.dsql.query;

import org.jetbrains.annotations.NotNull;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.ConnectionInformation;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

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
