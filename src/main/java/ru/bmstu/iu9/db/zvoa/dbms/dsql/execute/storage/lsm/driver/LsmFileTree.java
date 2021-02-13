package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class LsmFileTree<K extends Key, V extends Value> extends AbstractDbModule {

    private final File directory;
    private final LsmFile<K, V> file;

    public LsmFileTree(String path) throws DataStorageException {
        try {
            this.directory = new File(path);
            if (!directory.exists()) {
                directory.mkdir();
            }

            file = new LsmFile(path + "/file");
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
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

    public void put(K key, V value) throws DataStorageException {
        file.put(key, value);
    }

    public V readKey(K key) throws DataStorageException {
        return file.readKey(key);
    }

    public Map<K, V> readAllKeys() throws DataStorageException {
        return file.readAllKeys();
    }

    public Map<K, V> readKeysByNumbers(int start, int end) throws DataStorageException {
        return file.readKeysByNumbers(start, end);
    }

    public Map<K, V> readKeyBlock(K startKey, int blockSize) {
        return null;
    }
}
