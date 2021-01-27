package ru.bmstu.iu9.db.zvoa.dbms.modules;

public interface IDbStorage<T> {
    boolean put(T t) throws StorageException;

    T get() throws StorageException;
}
