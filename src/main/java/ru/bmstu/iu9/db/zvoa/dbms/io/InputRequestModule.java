package ru.bmstu.iu9.db.zvoa.dbms.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;

import java.util.function.Supplier;

/**
 * The type Input request module.
 *
 * @param <T> the type parameter
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class InputRequestModule<T> extends AbstractDbModule {
    private final static Logger LOGGER = LoggerFactory.getLogger(InputRequestModule.class);
    private final Supplier<T> queryGenerator;
    private final QueryRequestStorage queryRequestStorage;
    private final RequestHandler<T> requestHandler;

    /**
     * Instantiates a new Input request module.
     *
     * @param requestGenerator    the request generator
     * @param queryRequestStorage the query request storage
     * @param requestHandler      the request handler
     */
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
                Thread.onSpinWait();
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            setClosed();
            logClose();
        }
    }

    /**
     * Gets query request storage.
     *
     * @return the query request storage
     */
    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }
}
