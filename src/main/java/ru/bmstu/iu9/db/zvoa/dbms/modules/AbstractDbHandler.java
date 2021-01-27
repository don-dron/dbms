package ru.bmstu.iu9.db.zvoa.dbms.modules;

public abstract class AbstractDbHandler<T, R> implements IDbHandler<T, R> {
    @Override
    public abstract R execute(T t);
}
