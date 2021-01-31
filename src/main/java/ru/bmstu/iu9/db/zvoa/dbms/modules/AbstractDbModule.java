package ru.bmstu.iu9.db.zvoa.dbms.modules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.DBMS;
import ru.bmstu.iu9.db.zvoa.dbms.io.impl.DBMSServer;

public abstract class AbstractDbModule implements IDbModule {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private State state = State.NOT_INIT;

    @Override
    public abstract void init();

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

    protected void setInit() {
        state = State.INIT;
    }

    protected void setRunning() {
        state = State.RUNNING;
    }

    protected void setClosed() {
        state = State.CLOSED;
    }

    protected void logInit() {
        logger.info("Init " + getClass().getSimpleName() + " module.");
    }

    protected void logRunning() {
        logger.info("Running " + getClass().getSimpleName() + " module.");
    }

    protected void logClose() {
        logger.info("Close " + getClass().getSimpleName() + " module.");
    }

    private enum State {
        NOT_INIT,
        INIT,
        RUNNING,
        CLOSED
    }
}
