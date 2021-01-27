package ru.bmstu.iu9.db.zvoa.dbms.io;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.logging.Logger;

import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequest;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponse;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

public class InputRequestModule<T> extends AbstractDbModule {

    private final Supplier<T> requestGenerator;
    private final QueryRequestStorage queryRequestStorage;
    private final RequestHandler<T> requestHandler;
    private final Logger logger;

    public InputRequestModule(
            Supplier<T> requestGenerator,
            QueryRequestStorage queryRequestStorage,
            RequestHandler<T> requestHandler) {
        this.requestGenerator = requestGenerator;
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
            T request = requestGenerator.get();

            if (request != null) {
                queryRequestStorage.put(requestHandler.execute(request));
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
