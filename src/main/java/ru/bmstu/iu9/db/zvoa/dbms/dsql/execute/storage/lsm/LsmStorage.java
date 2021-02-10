package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LsmStorage extends AbstractDbModule implements IKeyValueStorage {
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

    @Override
    public void init() throws DataStorageException, IOException {

    }

    @Override
    public void run() {

    }

    @Override
    public void close() throws Exception {

    }
}
