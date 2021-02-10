package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public interface IKeyValueStorage {
    public void put(Key key, Value value);

    public Value get(Key key);

    public List<Value> getValues(Predicate<Map.Entry<Key, Value>> predicate);
}
