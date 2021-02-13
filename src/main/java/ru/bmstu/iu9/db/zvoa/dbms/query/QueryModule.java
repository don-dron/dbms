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
package ru.bmstu.iu9.db.zvoa.dbms.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.query.DSQLQueryHandler;

/**
 * The type Query module.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class QueryModule extends AbstractDbModule {
    private final Logger logger = LoggerFactory.getLogger(QueryModule.class);

    private final QueryRequestStorage queryRequestStorage;
    private final QueryResponseStorage queryResponseStorage;
    private final DSQLQueryHandler queryHandler;

    private QueryModule(Builder builder) {
        this.queryRequestStorage = builder.requestStorage;
        this.queryResponseStorage = builder.responseStorage;
        this.queryHandler = builder.handler;
    }

    /**
     * Gets query request storage.
     *
     * @return the query request storage
     */
    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }

    /**
     * Gets query response storage.
     *
     * @return the query response storage
     */
    public QueryResponseStorage getQueryResponseStorage() {
        return queryResponseStorage;
    }

    @Override
    public void init() {
        synchronized (this) {
            if (isInit()) {
                return;
            }
            setInit();
            logInit();
        }
    }

    @Override
    public void run() {
        synchronized (this) {
            if (isRunning()) {
                return;
            }
            if (isClosed()) {
                return;
            }
            setRunning();
            logRunning();
        }

        Query request;
        while (isRunning()) {
            if (!queryRequestStorage.isEmpty() && (request = queryRequestStorage.get()) != null) {
                logger.debug("Start execute query " + request);
                Query response = queryHandler.execute(request);
                logger.debug("End execute query " + request);
                queryResponseStorage.put(response);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            setClosed();
            logClose();
        }
    }

    /**
     * The type Builder.
     *
     * @author don-dron Zvorygin Andrey BMSTU IU-9
     */
    public static class Builder {
        private DSQLQueryHandler handler;
        private QueryRequestStorage requestStorage;
        private QueryResponseStorage responseStorage;

        /**
         * New builder builder.
         *
         * @return the builder
         */
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Sets query request storage.
         *
         * @param inputModule the input module
         * @return the query request storage
         */
        public Builder setQueryRequestStorage(QueryRequestStorage inputModule) {
            this.requestStorage = inputModule;
            return this;
        }

        /**
         * Sets query response storage.
         *
         * @param outputModule the output module
         * @return the query response storage
         */
        public Builder setQueryResponseStorage(QueryResponseStorage outputModule) {
            this.responseStorage = outputModule;
            return this;
        }

        /**
         * Sets query handler.
         *
         * @param queryHandler the query handler
         * @return the query handler
         */
        public Builder setQueryHandler(DSQLQueryHandler queryHandler) {
            this.handler = queryHandler;
            return this;
        }

        /**
         * Build query module.
         *
         * @return the query module
         */
        public QueryModule build() {
            return new QueryModule(this);
        }
    }
}
