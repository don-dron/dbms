package ru.bmstu.iu9.db.zvoa.dbms.modules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

import java.io.IOException;

/**
 * The type Abstract db module.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public abstract class AbstractDbModule implements IDbModule {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private volatile State state = State.NOT_INIT;

    @Override
    public abstract void init() throws DataStorageException, IOException;

    @Override
    public abstract void run();

    @Override
    public abstract void close() throws Exception;

    @Override
    public boolean isInit() {
        return state != State.NOT_INIT;
    }

    @Override
    public boolean isRunning() {
        return state == State.RUNNING;
    }

    @Override
    public boolean isClosed() {
        return state == State.CLOSED;
    }

    /**
     * Sets init.
     */
    protected void setInit() {
        state = State.INIT;
    }

    /**
     * Sets running.
     */
    protected void setRunning() {
        state = State.RUNNING;
    }

    /**
     * Sets closed.
     */
    protected void setClosed() {
        state = State.CLOSED;
    }

    /**
     * Log init.
     */
    protected void logInit() {
        logger.debug("Init " + getClass().getSimpleName() + " module.");
    }

    /**
     * Log running.
     */
    protected void logRunning() {
        logger.debug("Running " + getClass().getSimpleName() + " module.");
    }

    /**
     * Log close.
     */
    protected void logClose() {
        logger.debug("Close " + getClass().getSimpleName() + " module.");
    }

    private enum State {
        NOT_INIT,
        INIT,
        RUNNING,
        CLOSED
    }
}
