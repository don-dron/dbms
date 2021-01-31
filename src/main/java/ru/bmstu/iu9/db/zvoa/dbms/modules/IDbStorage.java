package ru.bmstu.iu9.db.zvoa.dbms.modules;

public interface IDbStorage<T> {
    boolean put(T t) throws StorageException;

    boolean isEmpty();

    T get() throws StorageException;
}
