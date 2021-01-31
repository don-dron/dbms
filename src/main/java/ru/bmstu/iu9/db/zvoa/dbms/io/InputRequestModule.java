package ru.bmstu.iu9.db.zvoa.dbms.io;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;

public class InputRequestModule<T> extends AbstractDbModule {
    private final static Logger LOGGER = LoggerFactory.getLogger(InputRequestModule.class);
    private final Supplier<T> queryGenerator;
    private final QueryRequestStorage queryRequestStorage;
    private final RequestHandler<T> requestHandler;

    public InputRequestModule(
            Supplier<T> requestGenerator,
            QueryRequestStorage queryRequestStorage,
            RequestHandler<T> requestHandler) {
        this.queryGenerator = requestGenerator;
        this.queryRequestStorage = queryRequestStorage;
        this.requestHandler = requestHandler;
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

        while (isRunning()) {
            T t = queryGenerator.get();

            if (t != null) {
                LOGGER.info("Input module handle request " + t);
                queryRequestStorage.put(requestHandler.execute(t));
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

    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }
}
