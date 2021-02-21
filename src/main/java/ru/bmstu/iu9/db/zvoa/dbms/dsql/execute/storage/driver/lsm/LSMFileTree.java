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

import com.google.common.base.Throwables;
import org.jetbrains.annotations.NotNull;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Value;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.ByteConverter;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Drive part for LSM Tree. Using for writing, merging and reading SSTables.
 *
 * @param <K> - type of keys
 * @param <V> - type of values
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class LSMFileTree<K extends Key, V extends Value> extends AbstractDbModule {
    private static final int LSM_TREE_DEGREE = 4;

    @NotNull
    private final File directory;

    @NotNull
    private final ByteConverter<K, V> byteConverter;

    @NotNull
    private final Map<Integer, ConcurrentLinkedDeque<SSTable<K, V>>> levelMap;

    @NotNull
    private final ReadWriteLock fileTreeLock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param byteConverter - byte converter for converting keys and values to/from bytes
     * @param path          - mount path
     */
    public LSMFileTree(@NotNull ByteConverter<K, V> byteConverter, @NotNull String path) {
        this.byteConverter = byteConverter;
        this.directory = new File(path);
        this.levelMap = new ConcurrentHashMap<>();

        if (!directory.exists()) {
            if (!directory.mkdir()) {
                throw new LSMFileCreatingException("Cannot create directory for LSM Storage " + path);
            }
        }

        Pattern pattern = Pattern.compile(SSTable.SSTABLE_FORMAT);

        File[] tables = directory.listFiles(file -> {
            Matcher matcher = pattern.matcher(file.getName());
            return matcher.find();
        });

        for (File file : tables) {
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
            throw new LSMRuntimeException("Merger crashed " + Throwables.getStackTraceAsString(exception));
        }
    }

    @Override
    public synchronized void close() {
        if (isClosed()) {
            return;
        }
        setClosed();
        logClose();
    }

    /**
     * Put records to lsm file storage.
     *
     * @param map - map with key and values
     */
    public void putAll(@NotNull Map<? extends K, ? extends V> map) {
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

    /**
     * Read all keys from lsm file storage
     *
     * @return - storage keys
     */
    public @NotNull Map<K, V> readAllKeys() {
        try {
            fileTreeLock.readLock().lock();
            return readAllKeys(levelMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        } finally {
            fileTreeLock.readLock().unlock();
        }
    }

    /**
     * Read {@code blockSize} keys with start offset {@code startKey}
     *
     * @param startKey  - start key
     * @param blockSize - block size
     * @return records
     */
    public @NotNull Map<K, V> readKeyBlock(@NotNull K startKey, int blockSize) {
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

    private boolean mayBeContains(Key key, SSTable<K, V> ssTable) {
        Key[] keys = ssTable.getMeta().getKeys();
        return keys.length != 0 && keys[0].compareTo(key) < 1 && key.compareTo(keys[keys.length - 1]) < 1;
    }

    private @NotNull Map<K, V> readAllKeys(@NotNull Collection<? extends SSTable<K, V>> tables) {
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
                .map(entry -> new Record<>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        int i = 0;
        while (true) {
            long shift = 64L * range(level);
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
            if (entry.getValue().size() >= LSM_TREE_DEGREE * LSM_TREE_DEGREE) {
                merge(entry.getKey());
                flag = true;
            }
        }
        return flag;
    }

    private long range(int level) {
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
            while (isRunning()) {
                if (!tryMerge()) {
                    Thread.yield();
                }
            }

            tryMerge();
        }
    }

    @Override
    public String toString() {
        return "LSMFileTree{" +
                "directory=" + directory +
                ", byteConverter=" + byteConverter +
                ", levelMap=" + levelMap +
                ", fileTreeLock=" + fileTreeLock +
                "}";
    }
}
