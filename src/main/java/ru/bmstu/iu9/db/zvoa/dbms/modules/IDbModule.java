package ru.bmstu.iu9.db.zvoa.dbms.modules;

/**
 * The interface Db module.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public interface IDbModule extends AutoCloseable, Runnable {

    /**
     * Init.
     */
    void init();

    @Override
    void run();

    @Override
    void close() throws Exception;

    /**
     * Is init boolean.
     *
     * @return the boolean
     */
    boolean isInit();

    /**
     * Is running boolean.
     *
     * @return the boolean
     */
    boolean isRunning();

    /**
     * Is closed boolean.
     *
     * @return the boolean
     */
    boolean isClosed();
}
