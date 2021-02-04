package ru.bmstu.iu9.db.zvoa.dbms.modules;

/**
 * The type Abstract db handler.
 *
 * @param <T> the type parameter
 * @param <R> the type parameter
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public abstract class AbstractDbHandler<T, R> implements IDbHandler<T, R> {
    @Override
    public abstract R execute(T t);
}
