package ru.bmstu.iu9.db.zvoa.dbms.io;

import java.util.function.Supplier;
import java.util.logging.Logger;

import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;

public class InputRequestModule<T> extends AbstractDbModule {

    private final Supplier<T> queryGenerator;
    private final QueryRequestStorage queryRequestStorage;
    private final RequestHandler<T> requestHandler;
    private final Logger logger;

    public InputRequestModule(
            Supplier<T> requestGenerator,
            QueryRequestStorage queryRequestStorage,
            RequestHandler<T> requestHandler) {
        this.queryGenerator = requestGenerator;
        this.queryRequestStorage = queryRequestStorage;
        this.requestHandler = requestHandler;
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
        while (isRunning()) {
//            System.out.println(getClass().getSimpleName());
            T t = queryGenerator.get();

            if (t != null) {
                logger.info("Input module handle request " + t);
                queryRequestStorage.put(requestHandler.execute(t));
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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

    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }
}
