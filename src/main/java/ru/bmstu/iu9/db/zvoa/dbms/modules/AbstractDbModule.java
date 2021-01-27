package ru.bmstu.iu9.db.zvoa.dbms.modules;

import java.util.logging.Logger;

public abstract class AbstractDbModule implements IDbModule {
    private Logger logger = Logger.getLogger(getClass().getName());

    private boolean isInit;
    private boolean isRunning;
    private boolean isClosed;

    @Override
    public abstract void init();

    @Override
    public abstract void run();

    @Override
    public abstract void close() throws Exception;

    @Override
    public boolean isInit() {
        return isInit;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    protected Logger getLogger() {
        return logger;
    }

    protected void setInit() {
        isInit = true;
    }

    protected void setRunning() {
        isRunning = true;
    }

    protected void setClosed() {
        isClosed = true;
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
}
