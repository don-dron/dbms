package ru.bmstu.iu9.db.zvoa.dbms.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

/**
 * The type Query module.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class QueryModule extends AbstractDbModule {
    private final static Logger LOGGER = LoggerFactory.getLogger(QueryModule.class);

    private final QueryRequestStorage queryRequestStorage;
    private final QueryResponseStorage queryResponseStorage;
    private final QueryHandler queryHandler;

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
            setRunning();
            logRunning();
        }

        Query request;
        while (isRunning()) {
            if (!queryRequestStorage.isEmpty() && (request = queryRequestStorage.get()) != null) {
                Query response = queryHandler.execute(request);
                queryResponseStorage.put(response);
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    LOGGER.warn(e.getMessage());
                }
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
        private QueryHandler handler;
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
        public Builder setQueryHandler(QueryHandler queryHandler) {
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
