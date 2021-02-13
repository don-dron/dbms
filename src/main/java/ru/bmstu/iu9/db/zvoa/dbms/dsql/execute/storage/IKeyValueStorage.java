package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbModule;

import java.util.Map;
import java.util.function.Predicate;

public interface IKeyValueStorage<K extends Key, V extends Value> extends IDbModule {
    public void put(K key, V value) throws DataStorageException;

    public V get(K key) throws DataStorageException;

    public Map<K, V> getValues(Predicate<Map.Entry<K, V>> predicate) throws DataStorageException;
}
