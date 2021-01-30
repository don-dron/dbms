package ru.bmstu.iu9.db.zvoa.dbms.query;

import java.util.logging.Logger;

import ru.bmstu.iu9.db.zvoa.dbms.DBMS;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

public class QueryModule extends AbstractDbModule {

    private final QueryRequestStorage queryRequestStorage;
    private final QueryResponseStorage queryResponseStorage;
    private final QueryHandler queryHandler;
    private final Logger logger;

    private QueryModule(Builder builder) {
        this.queryRequestStorage = builder.requestStorage;
        this.queryResponseStorage = builder.responseStorage;
        this.queryHandler = builder.handler;
        this.logger = getLogger();
    }

    @Override
    public void init() {
        setInit();
        logInit();
    }

    @Override
    public synchronized void run() {
        if (isRunning()) {
            return;
        }
        setRunning();
        logRunning();

        while (!isClosed()) {
            Query request = queryRequestStorage.get();

            if (request != null) {
                Query response = queryHandler.execute(request);
                queryResponseStorage.put(response);
            } else {
                Thread.yield();
            }
        }
    }

    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }

    public QueryResponseStorage getQueryResponseStorage() {
        return queryResponseStorage;
    }

    @Override
    public void close() throws Exception {
        setClosed();
        logClose();
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
