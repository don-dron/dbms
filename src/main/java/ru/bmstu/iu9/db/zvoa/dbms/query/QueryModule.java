package ru.bmstu.iu9.db.zvoa.dbms.query;

import java.util.logging.Logger;

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

    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }

    public QueryResponseStorage getQueryResponseStorage() {
        return queryResponseStorage;
    }

    @Override
    public synchronized void init() {
        if (isInit()) {
            return;
        }
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

        Query request;
        while (isRunning()) {
//            System.out.println(getClass().getSimpleName());
            if (!queryRequestStorage.isEmpty() && (request = queryRequestStorage.get()) != null) {
                Query response = queryHandler.execute(request);
                queryResponseStorage.put(response);
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                try {
//                    queryRequestStorage.wait();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (isClosed()) {
            return;
        }
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
