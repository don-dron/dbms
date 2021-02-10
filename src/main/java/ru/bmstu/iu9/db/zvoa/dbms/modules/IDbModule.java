package ru.bmstu.iu9.db.zvoa.dbms.modules;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

import java.io.IOException;

/**
 * The interface Db module.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public interface IDbModule extends AutoCloseable, Runnable {

    /**
     * Init.
     */
    void init() throws DataStorageException, IOException;

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
