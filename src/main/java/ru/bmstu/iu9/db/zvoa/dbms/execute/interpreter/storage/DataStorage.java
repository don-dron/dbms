package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import java.util.List;

public interface DataStorage {
    public void createTable(String tableName, List<Type> types) throws DataStorageException;

    public void put(String tableName, List<Object> values) throws DataStorageException;

    public void get(String tableName, Object key) throws DataStorageException;

    public void remove(String tableName, Object key) throws DataStorageException;
}
