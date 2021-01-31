package ru.bmstu.iu9.db.zvoa.dbms.io;

import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.logging.Logger;

import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
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
        Query response;
        while (isRunning()) {
//            System.out.println(getClass().getSimpleName());
            if (!queryResponseStorage.isEmpty() &&
                    (response = queryResponseStorage.get()) != null) {
                T t = responseHandler.execute(response);
                logger.info("Output module handle response " + t);
                consumer.accept(t);
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                try {
//                    queryResponseStorage.wait();
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

    public QueryResponseStorage getQueryResponseStorage() {
        return queryResponseStorage;
    }
}
