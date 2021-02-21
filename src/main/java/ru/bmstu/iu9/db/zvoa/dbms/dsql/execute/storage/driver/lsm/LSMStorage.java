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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.lsm;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.StorageProperties;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Value;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.ByteConverter;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Lsm storage implementation.
 *
 * @param <K> - type of keys
 * @param <V> - type of values
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class LSMStorage<K extends Key, V extends Value> extends AbstractDbModule implements IKeyValueStorage<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LSMStorage.class);
    private static final int MAX_CACHE_SIZE = 100;

    private final String path;
    private final LSMFileTree<K, V> lsmFileTree;
    private final ReadWriteLock memoryLock = new ReentrantReadWriteLock();
    private final ReadWriteLock fileTreeLock = new ReentrantReadWriteLock();
    private final ByteConverter<K, V> byteConverter;
    private Map<K, V> lsmMemory;
    private LSMLogfile<K, V> LSMLogfile;

    /**
     * Constructor.
     *
     * @param storageProperties - storage properties
     */
    public LSMStorage(@NotNull StorageProperties<K, V> storageProperties) {
        this.path = storageProperties.getPath();
        this.byteConverter = storageProperties.getByteConverter();
        this.lsmFileTree = new LSMFileTree<>(storageProperties.getByteConverter(), storageProperties.getPath());
        this.LSMLogfile = new LSMLogfile<>(storageProperties.getByteConverter(), storageProperties.getPath() + "/log");
        lsmMemory = LSMLogfile.getLastData();
    }

    @Override
    public synchronized void init() {
        if (isInit()) {
            return;
        }
        setInit();
        logInit();
        LOGGER.debug("Init storage {}", path);
    }

    @Override
    public void run() {
        ExecutorService executorService;
        synchronized (this) {
            if (isRunning()) {
                return;
            }

            if (isClosed()) {
                return;
            }
            setRunning();
            logRunning();

            LOGGER.debug("Run storage {}", path);

            executorService = Executors.newCachedThreadPool();
            executorService.submit(() -> {
                try {
                    while (isRunning()) {
                        if (lsmMemory.size() >= 64 || LSMLogfile.getCount() >= 1024) {
                            pushToDrive();
                        } else {
                            Thread.yield();
                        }
                    }
                    pushToDrive();
                } catch (Exception exception) {
                    LOGGER.error(exception.getMessage());
                }
            });

            executorService.submit(lsmFileTree);
        }
        executorService.shutdown();
        joinExecutorService(executorService);
    }

    @Override
    public synchronized void close() {
        if (isClosed()) {
            return;
        }
        lsmFileTree.close();
        setClosed();
        logClose();
        LOGGER.debug("Close storage {}", path);
    }

    /**
     * Push data to drive.
     */
    public void pushToDrive() {
        memoryLock.writeLock().lock();
        fileTreeLock.writeLock().lock();

        try {
            Map<K, V> map = lsmMemory;
            lsmFileTree.putAll(map);
            lsmMemory = new TreeMap<>();
            LSMLogfile.delete();
            LSMLogfile = new LSMLogfile<>(byteConverter, path + "/log");
        } finally {
            fileTreeLock.writeLock().unlock();
            memoryLock.writeLock().unlock();
        }
    }

    private void joinExecutorService(ExecutorService executorService) {
        while (true) {
            try {
                if (executorService.awaitTermination(0, TimeUnit.MILLISECONDS)) {
                    break;
                } else {
                    Thread.yield();
                }
            } catch (InterruptedException exception) {
                LOGGER.warn(exception.getMessage());
            }
        }
    }

    private ExecutorService createExecutorService() {
        return new ThreadPoolExecutor(
                16,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                TimeUnit.DAYS,
                new SynchronousQueue<>());
    }

    @Override
    public V put(K key, V value) {
        memoryLock.writeLock().lock();
        try {
            LSMLogfile.put(key, value);
            return lsmMemory.put(key, value);
        } finally {
            memoryLock.writeLock().unlock();
        }
    }

    @Override
    public V get(K key) {
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

            try {
                lsmMemory = lsmFileTree.readAllKeys();
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
    public Map<K, V> getValues(Predicate<? super Map.Entry<K, V>> predicate) {
        memoryLock.readLock().lock();
        fileTreeLock.readLock().lock();
        try {
            Map<K, V> inFile = lsmFileTree.readAllKeys();
            Map<K, V> inMemory = lsmMemory.entrySet().stream().filter(predicate).collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (a, b) -> a,
                    TreeMap::new));

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

    @Override
    public String toString() {
        return "LsmStorage{" +
                "path=" + path +
                "}";
    }
}
