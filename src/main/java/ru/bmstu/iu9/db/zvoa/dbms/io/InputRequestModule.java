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
    public void init() {
        setInit();
        logInit();
    }

    @Override
    public void run() {
        setRunning();
        logRunning();
        while (isRunning() && !isClosed()) {
            T t = queryGenerator.get();

            if (t != null) {
                logger.info("Input module handle request " + t);
                queryRequestStorage.put(requestHandler.execute(t));
            } else {
                Thread.yield();
            }
        }
    }

    @Override
    public void close() throws Exception {
        setClosed();
        logClose();
    }

    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }
}
