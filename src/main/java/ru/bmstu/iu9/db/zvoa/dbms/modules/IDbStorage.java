package ru.bmstu.iu9.db.zvoa.dbms.modules;

/**
 * The interface Db storage.
 *
 * @param <T> the type parameter
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public interface IDbStorage<T> {
    /**
     * Put boolean.
     *
     * @param t the t
     * @return the boolean
     * @throws StorageException the storage exception
     */
    boolean put(T t) throws StorageException;

    /**
     * Is empty boolean.
     *
     * @return the boolean
     */
    boolean isEmpty();

    /**
     * Get t.
     *
     * @return the t
     * @throws StorageException the storage exception
     */
    T get() throws StorageException;
}
