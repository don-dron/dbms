package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.StorageProperties;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver.LsmFileTree;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.memory.LsmMemory;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LsmStorage<K extends Key, V extends Value> extends AbstractDbModule implements IKeyValueStorage<K, V> {

    private static final int MAX_CACHE_SIZE = 100;

    private final LsmMemory<K, V> lsmMemory;
    private final LsmFileTree<K, V> lsmFileTree;
    private final LsmCacheAlgorithm<K, V> lsmCacheAlgorithm = new LsmCacheAlgorithmHalf<>();

    public LsmStorage(StorageProperties storageProperties) throws DataStorageException {
        this.lsmFileTree = new LsmFileTree(storageProperties.getPath());
        this.lsmMemory = new LsmMemory();
    }

    @Override
    public void init() throws DataStorageException, IOException {
        lsmMemory.init();
    }

    @Override
    public void run() {
        Map<K, V> map = lsmMemory.pushToDrive();
        map.entrySet().forEach(entry -> {
            try {
                lsmFileTree.put(entry.getKey(), entry.getValue());
            } catch (DataStorageException dataStorageException) {
                dataStorageException.printStackTrace();
            }
        });
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void put(K key, V value) throws DataStorageException {
        lsmMemory.put(key, value);
        run();
    }

    @Override
    public V get(K key) throws DataStorageException {
        V value = lsmMemory.get(key);

        if (value == null) {
            Map<K, V> map = new TreeMap<>();
            map.put(key, lsmFileTree.readKey(key));
            lsmMemory.updateCache(lsmCacheAlgorithm, map);
            value = lsmMemory.get(key);
            return value;
        } else {
            return value;
        }
    }

    @Override
    public Map<K, V> getValues(Predicate<Map.Entry<K, V>> predicate) throws DataStorageException {
        Map<K, V> inFile = lsmFileTree.readAllKeys()
                .entrySet()
                .stream()
                .filter(predicate)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<K, V> inMemory = lsmMemory.getValues(predicate);

        Map<K, V> result =
                inFile.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (v1, v2) -> v1,
                                () -> new TreeMap<>(inMemory)));
        return result;
    }
}
