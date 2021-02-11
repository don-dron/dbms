package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.LsmCacheAlgorithm;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LsmMemory<K extends Key, V extends Value> extends AbstractDbModule implements IKeyValueStorage<K, V> {

    private Map<K, V> map;

    @Override
    public void put(K key, V value) throws DataStorageException {
        map.put(key, value);
    }

    @Override
    public V get(K key) throws DataStorageException {
        return map.get(key);
    }

    @Override
    public Map<K, V> getValues(Predicate<Map.Entry<K, V>> predicate) throws DataStorageException {
        return map.entrySet().stream().filter(predicate).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void updateCache(LsmCacheAlgorithm lsmCacheAlgorithm, Map<K, V> newData) {
        Map<K, V> afterSwap = lsmCacheAlgorithm.swapMemory(map, newData);
        Map<K, V> oldMap = map;
        map = afterSwap;
    }

    @Override
    public void init() throws DataStorageException, IOException {
        map = new TreeMap<>();
    }

    @Override
    public void run() {

    }

    @Override
    public void close() throws Exception {

    }
}
