package ru.bmstu.iu9.db.zvoa.dbms.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

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

    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }

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
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            setClosed();
            logClose();
        }
    }

    public static class Builder {
        private QueryHandler handler;
        private QueryRequestStorage requestStorage;
        private QueryResponseStorage responseStorage;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setQueryRequestStorage(QueryRequestStorage inputModule) {
            this.requestStorage = inputModule;
            return this;
        }

        public Builder setQueryResponseStorage(QueryResponseStorage outputModule) {
            this.responseStorage = outputModule;
            return this;
        }

        public Builder setQueryHandler(QueryHandler queryHandler) {
            this.handler = queryHandler;
            return this;
        }

        public QueryModule build() {
            return new QueryModule(this);
        }
    }
}
