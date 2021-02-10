package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LsmStorage implements IKeyValueStorage {
    private TreeMap<Key, Value> treeMap = new TreeMap<>();

    public LsmStorage(Path mount) {

    }

    @Override
    public synchronized void put(Key key, Value value) {
        treeMap.put(key, value);
    }

    @Override
    public synchronized Value get(Key key) {
        return treeMap.get(key);
    }

    @Override
    public synchronized List<Value> getValues(Predicate<Map.Entry<Key, Value>> predicate) {
        return treeMap.entrySet().stream()
                .filter(predicate)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }
}
