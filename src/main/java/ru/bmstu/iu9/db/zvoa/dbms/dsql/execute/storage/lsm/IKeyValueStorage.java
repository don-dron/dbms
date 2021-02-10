package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbModule;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public interface IKeyValueStorage extends IDbModule {
    public void put(Key key, Value value) throws DataStorageException;

    public Value get(Key key) throws DataStorageException;

    public List<Value> getValues(Predicate<Map.Entry<Key, Value>> predicate) throws DataStorageException;
}
