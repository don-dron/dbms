package ru.bmstu.iu9.db.zvoa.dbms.modules;

public interface IDbHandler<T, R> {
    public R execute(T t) throws HandleException;
}
