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

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LsmFileTree<K extends Key, V extends Value> extends AbstractDbModule {

    private final File directory;
    private final Map<Integer, ConcurrentLinkedDeque<SSTable<K, V>>> levelMap = new ConcurrentHashMap<>();
    private final ReadWriteLock fileTreeLock = new ReentrantReadWriteLock();
    private int POW = 4;

    public LsmFileTree(String path) throws DataStorageException {
        try {
            this.directory = new File(path);
            if (!directory.exists()) {
                directory.mkdir();
            }

            for (File file : Objects.requireNonNull(directory.listFiles(file -> file.getName().startsWith("sstable")))) {
                String[] strings = file.getName().split("_");
                int level = Integer.parseInt(strings[1]);
                SSTable<K, V> ssTable = new SSTable<>(
                        path,
                        level,
                        Integer.parseInt(strings[2]));
                addTable(level, ssTable);
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    @Override
    public synchronized void init() throws DataStorageException, IOException {
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

    public class Merger extends Thread {
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

    private void addTable(int level, SSTable<K, V> ssTable) {
        if (!levelMap.containsKey(level)) {
            levelMap.put(level, new ConcurrentLinkedDeque<>());
        }

        levelMap.get(level).add(ssTable);
    }

    private boolean tryMerge() {
        boolean flag = false;
        for (Map.Entry<Integer, ConcurrentLinkedDeque<SSTable<K, V>>> entry : levelMap.entrySet()) {
            try {
                if (entry.getValue().size() >= POW * POW) {
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
        int i = POW;

        while (level != 0) {
            i *= POW;
            level--;
        }

        return i;
    }

    private void merge(int level) throws DataStorageException {
        List<SSTable<K, V>> tables = new ArrayList<>(levelMap.get(level));
        List<Record> records = readAllKeys()
                .entrySet()
                .stream()
                .map(entry -> new Record<K, V>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        int i = 0;
        while (true) {
            int coef = range(level);
            int shift = 64 * coef;
            List<Record> recordList = records.stream().skip(i).limit(shift).collect(Collectors.toList());

            if (recordList.isEmpty()) {
                break;
            }

            try {
                if (levelMap.get(level + 1) == null) {
                    levelMap.put(level + 1, new ConcurrentLinkedDeque<>());
                }

                SSTable<K, V> ssTable = new SSTable<>(directory.getAbsolutePath(), level + 1,
                        levelMap.get(level + 1)
                                .stream()
                                .map(SSTable::getIndex)
                                .max(Integer::compareTo)
                                .orElse(0) + 1);

                ssTable.putAll(recordList.toArray(Record[]::new));

                addTable(level + 1, ssTable);
            } catch (DataStorageException dataStorageException) {
                dataStorageException.printStackTrace();
            }

            i += shift;
        }

        tables.forEach(table -> {
            levelMap.get(table.getLevel()).remove(table);
            table.delete();
        });
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            setClosed();
            logClose();
        }
    }

    public void putAll(Map<K, V> map) throws DataStorageException {
        try {
            fileTreeLock.writeLock().lock();

            if (levelMap.get(0) == null) {
                levelMap.put(0, new ConcurrentLinkedDeque<>());
            }

            SSTable<K, V> ssTable = new SSTable<>(directory.getAbsolutePath(), 0,
                    levelMap.get(0)
                            .stream()
                            .map(SSTable::getIndex)
                            .max(Integer::compareTo)
                            .orElse(0) + 1);
            ssTable.putAll(map.entrySet().stream().map(entry -> new Record(entry.getKey(), entry.getValue())).toArray(Record[]::new));

            addTable(0, ssTable);
        } finally {
            fileTreeLock.writeLock().unlock();
        }
    }

    public Map<K, V> readAllKeys() throws DataStorageException {
        try {
            fileTreeLock.readLock().lock();
            return readAllKeys(levelMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        } finally {
            fileTreeLock.readLock().unlock();
        }
    }

    public Map<K, V> readAllKeys(Collection<SSTable<K, V>> tables) throws DataStorageException {
        return tables.stream()
                .flatMap(ssTable -> {
                            try {
                                return Arrays.stream(ssTable.readAllRecords());
                            } catch (DataStorageException dataStorageException) {
                                dataStorageException.printStackTrace();
                                return Stream.empty();
                            }
                        }
                )
                .sorted(Comparator.comparingLong(Record::getTimestamp))
                .collect(Collectors.toMap(
                        Record::getKey,
                        Record::getValue,
                        (v1, v2) -> v1,
                        TreeMap::new));
    }

    public Map<K, V> readKeysByNumbers(int start, int end) throws DataStorageException {
        return null;
    }

    public Map<K, V> readKeyBlock(K startKey, int blockSize) {
        return null;
    }
}
