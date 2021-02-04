package ru.bmstu.iu9.db.zvoa.dbms.modules;

/**
 * The interface Db handler.
 *
 * @param <T> the type parameter
 * @param <R> the type parameter
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public interface IDbHandler<T, R> {
    /**
     * Execute r.
     *
     * @param t the t
     * @return the r
     * @throws HandleException the handle exception
     */
    public R execute(T t) throws HandleException;
}
