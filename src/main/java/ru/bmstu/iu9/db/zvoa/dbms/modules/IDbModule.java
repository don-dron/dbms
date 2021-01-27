package ru.bmstu.iu9.db.zvoa.dbms.modules;

public interface IDbModule extends AutoCloseable, Runnable {

    void init();

    @Override
    void run();

    @Override
    void close() throws Exception;

    boolean isInit();

    boolean isRunning();

    boolean isClosed();
}
