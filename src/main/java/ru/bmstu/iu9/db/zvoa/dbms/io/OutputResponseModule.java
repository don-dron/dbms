package ru.bmstu.iu9.db.zvoa.dbms.io;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

public class OutputResponseModule<T> extends AbstractDbModule {
    private final static Logger LOGGER = LoggerFactory.getLogger(OutputResponseModule.class);

    private final QueryResponseStorage queryResponseStorage;
    private final ResponseHandler<T> responseHandler;
    private final Consumer<T> consumer;

    public OutputResponseModule(
            Consumer<T> consumer,
            QueryResponseStorage queryResponseStorage,
            ResponseHandler<T> responseHandler) {
        this.consumer = consumer;
        this.queryResponseStorage = queryResponseStorage;
        this.responseHandler = responseHandler;
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

        Query response;
        while (isRunning()) {
            if (!queryResponseStorage.isEmpty() &&
                    (response = queryResponseStorage.get()) != null) {
                T t = responseHandler.execute(response);
                LOGGER.info("Output module handle response " + t);
                consumer.accept(t);
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

    public QueryResponseStorage getQueryResponseStorage() {
        return queryResponseStorage;
    }
}
