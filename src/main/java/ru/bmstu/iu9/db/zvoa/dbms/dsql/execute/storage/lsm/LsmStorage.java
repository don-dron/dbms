/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.DBMSDataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.StorageProperties;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver.LsmFileTree;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.memory.LsmMemory;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LsmStorage<K extends Key, V extends Value> extends AbstractDbModule implements IKeyValueStorage<K, V> {
    private final Logger logger = LoggerFactory.getLogger(DBMSDataStorage.class);
    private static final int MAX_CACHE_SIZE = 100;

    private final String path;
    private final LsmMemory<K, V> lsmMemory;
    private final LsmFileTree<K, V> lsmFileTree;
    private final LsmCacheAlgorithm<K, V> lsmCacheAlgorithm = new LsmCacheAlgorithmAll<>();
    private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();
    private final ReadWriteLock fileTreeLock = new ReentrantReadWriteLock();

    public LsmStorage(StorageProperties storageProperties) throws DataStorageException {
        this.path = storageProperties.getPath();
        this.lsmFileTree = new LsmFileTree(storageProperties.getPath());
        this.lsmMemory = new LsmMemory();
    }

    @Override
    public synchronized void init() throws DataStorageException, IOException {
        if (isInit()) {
            return;
        }

        lsmMemory.init();
        setInit();
        logInit();
        logger.debug("Init storage " + path);
    }

    @Override
    public void run() {
        synchronized (this) {
            if (isRunning()) {
                return;
            }

            if (isClosed()) {
                return;
            }
            setRunning();
            logRunning();
            logger.debug("Run storage " + path);
        }

        try {
            while (isRunning()) {
                if (lsmMemory.size() >= 4096) {
                    pushToDrive();
                } else {
                    Thread.yield();
                }
            }
            pushToDrive();
        } catch (DataStorageException dataStorageException) {
            dataStorageException.printStackTrace();
        }
    }

    public void pushToDrive() throws DataStorageException {
        memoryLock.writeLock().lock();
        fileTreeLock.writeLock().lock();

        try {
            Map<K, V> map = lsmMemory.swapMemory();
            lsmFileTree.putAll(map);
        } finally {
            fileTreeLock.writeLock().unlock();
            memoryLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            setClosed();
            logClose();
            logger.debug("Close storage " + path);
        }
    }

    @Override
    public V put(K key, V value) throws DataStorageException {
        memoryLock.writeLock().lock();
        try {
            return lsmMemory.put(key, value);
        } finally {
            memoryLock.writeLock().unlock();
        }
    }

    @Override
    public V get(K key) throws DataStorageException {
        memoryLock.readLock().lock();
        V value;
        try {
            value = lsmMemory.get(key);
        } finally {
            memoryLock.readLock().unlock();
        }

        if (value == null) {
            memoryLock.writeLock().lock();
            fileTreeLock.readLock().lock();
            Map<K, V> map = lsmFileTree.readAllKeys();
            try {
                lsmMemory.updateCache(lsmCacheAlgorithm, map);
                value = lsmMemory.get(key);

                return value;
            } finally {
                fileTreeLock.readLock().unlock();
                memoryLock.writeLock().unlock();
            }
        } else {
            return value;
        }
    }

    @Override
    public Map<K, V> getValues(Predicate<Map.Entry<K, V>> predicate) throws DataStorageException {
        memoryLock.readLock().lock();
        fileTreeLock.readLock().lock();
        try {
            Map<K, V> inFile = lsmFileTree.readAllKeys();
            Map<K, V> inMemory = lsmMemory.getValues(predicate);

            return inFile.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (v1, v2) -> v1,
                            () -> new TreeMap<>(inMemory)));
        } finally {
            fileTreeLock.readLock().unlock();
            memoryLock.readLock().unlock();
        }
    }
}
