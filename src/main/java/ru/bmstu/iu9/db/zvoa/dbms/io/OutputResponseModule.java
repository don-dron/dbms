package ru.bmstu.iu9.db.zvoa.dbms.io;

import java.util.function.Consumer;
import java.util.logging.Logger;

import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponse;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

public class OutputResponseModule<T> extends AbstractDbModule {

    private final QueryResponseStorage queryResponseStorage;
    private final ResponseHandler<T> responseHandler;
    private final Logger logger;
    private final Consumer<T> consumer;

    public OutputResponseModule(
            Consumer<T> consumer,
            QueryResponseStorage queryResponseStorage,
            ResponseHandler<T> responseHandler) {
        this.consumer = consumer;
        this.queryResponseStorage = queryResponseStorage;
        this.responseHandler = responseHandler;
        this.logger = getLogger();
    }

    @Override
    public void init() {
        setInit();
        logInit();
    }

    @Override
    public void run() {
        setRunning();
        logRunning();
        while (isRunning() && !isClosed()) {
            QueryResponse response = queryResponseStorage.get();

            if (response != null) {
                consumer.accept(responseHandler.execute(response));
            }
        }
    }

    @Override
    public void close() throws Exception {
        setClosed();
        logClose();
    }

    public QueryResponseStorage getQueryResponseStorage() {
        return queryResponseStorage;
    }
}
