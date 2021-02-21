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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver;

import org.jetbrains.annotations.NotNull;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Drive part for LSM Tree. Using for writing, merging and reading SSTables.
 *
 * @param <K> - type of keys
 * @param <V> - type of values
 */
public class LSMFileTree<K extends Key, V extends Value> extends AbstractDbModule {
    private static final int LSM_TREE_DEGREE = 4;

    private final File directory;
    private final ByteConverter<K, V> byteConverter;
    private final Map<Integer, ConcurrentLinkedDeque<SSTable<K, V>>> levelMap = new ConcurrentHashMap<>();
    private final ReadWriteLock fileTreeLock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param byteConverter - byte converter for converting keys and values to/from bytes
     * @param path          - mount path
     */
    public LSMFileTree(ByteConverter<K, V> byteConverter, String path) {
        this.byteConverter = byteConverter;
        this.directory = new File(path);
        if (!directory.exists()) {
            if (!directory.mkdir()) {
                throw new LSMFileCreatingException("Cannot create directory for LSM Storage " + path);
            }
        }

        for (File file : Objects.requireNonNull(directory.listFiles(file -> file.getName().startsWith("sstable")))) {
            String[] strings = file.getName().split("_");
            int level = Integer.parseInt(strings[1]);
            SSTable<K, V> ssTable = new SSTable<>(
                    byteConverter,
                    path,
                    level,
                    Integer.parseInt(strings[2]));
            addTable(level, ssTable);
        }
    }

    @Override
    public synchronized void init() {
        if (isInit()) {
            return;
        }

        setInit();
        logInit();
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
        }

        try {
            Merger merger = new Merger();
            merger.start();
            merger.join();
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            setClosed();
            logClose();
        }
    }

    public void putAll(Map<K, V> map) {
        try {
            fileTreeLock.writeLock().lock();

            if (levelMap.get(0) == null) {
                levelMap.put(0, new ConcurrentLinkedDeque<>());
            }

            SSTable<K, V> ssTable = new SSTable<>(byteConverter,
                    directory.getAbsolutePath(),
                    0,
                    levelMap.get(0)
                            .stream()
                            .map(SSTable::getIndex)
                            .max(Integer::compareTo)
                            .orElse(0) + 1);
            ssTable.putAll(map.entrySet()
                    .stream()
                    .map(entry -> new Record(entry.getKey(), entry.getValue()))
                    .toArray(Record[]::new));

            addTable(0, ssTable);
        } finally {
            fileTreeLock.writeLock().unlock();
        }
    }

    public Map<K, V> readAllKeys() {
        try {
            fileTreeLock.readLock().lock();
            return readAllKeys(levelMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        } finally {
            fileTreeLock.readLock().unlock();
        }
    }

    public boolean mayBeContains(Key key, SSTable<K, V> ssTable) {
        Key[] keys = ssTable.getMeta().getKeys();
        boolean result = keys.length != 0 && keys[0].compareTo(key) < 1 && key.compareTo(keys[keys.length - 1]) < 1;
        return result;
    }

    public Map<K, V> readKeyBlock(K startKey, int blockSize) {
        try {
            fileTreeLock.readLock().lock();
            return readAllKeys(levelMap
                    .values()
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(ssTable -> mayBeContains(startKey, ssTable))
                    .collect(Collectors.toList()));
        } finally {
            fileTreeLock.readLock().unlock();
        }
    }

    private @NotNull Map<K, V> readAllKeys(@NotNull Collection<SSTable<K, V>> tables) {
        return tables.stream()
                .flatMap(ssTable -> Arrays.stream(ssTable.readAllRecords()))
                .sorted(Comparator.comparingLong(Record::getTimestamp))
                .collect(Collectors.toMap(
                        Record::getKey,
                        Record::getValue,
                        (v1, v2) -> v1,
                        TreeMap::new));
    }

    private void merge(int level) {
        List<SSTable<K, V>> tables = new ArrayList<>(levelMap.get(level));
        List<Record<K, V>> records = readAllKeys()
                .entrySet()
                .stream()
                .map(entry -> new Record<K, V>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        int i = 0;
        while (true) {
            int shift = 64 * range(level);
            List<Record<K, V>> recordList = records.stream().skip(i).limit(shift).collect(Collectors.toList());

            if (recordList.isEmpty()) {
                break;
            }

            if (levelMap.get(level + 1) == null) {
                levelMap.put(level + 1, new ConcurrentLinkedDeque<>());
            }

            SSTable<K, V> ssTable = new SSTable<>(
                    byteConverter,
                    directory.getAbsolutePath(), level + 1,
                    levelMap.get(level + 1)
                            .stream()
                            .map(SSTable::getIndex)
                            .max(Integer::compareTo)
                            .orElse(0) + 1);

            ssTable.putAll(recordList.toArray(Record[]::new));
            addTable(level + 1, ssTable);

            i += shift;
        }

        tables.forEach(table -> {
            levelMap.get(table.getLevel()).remove(table);
            table.delete();
        });
    }

    private void addTable(int level, @NotNull SSTable<K, V> ssTable) {
        if (!levelMap.containsKey(level)) {
            levelMap.put(level, new ConcurrentLinkedDeque<>());
        }

        levelMap.get(level).add(ssTable);
    }

    private boolean tryMerge() {
        boolean flag = false;
        for (Map.Entry<Integer, ConcurrentLinkedDeque<SSTable<K, V>>> entry : levelMap.entrySet()) {
            try {
                if (entry.getValue().size() >= LSM_TREE_DEGREE * LSM_TREE_DEGREE) {
                    merge(entry.getKey());
                    flag = true;
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        return flag;
    }

    private int range(int level) {
        int i = LSM_TREE_DEGREE;

        while (level != 0) {
            i *= LSM_TREE_DEGREE;
            level--;
        }

        return i;
    }

    private class Merger extends Thread {
        @Override
        public void run() {
            while (LSMFileTree.this.isRunning()) {
                if (!tryMerge()) {
                    Thread.yield();
                }
            }

            tryMerge();
        }
    }
}
