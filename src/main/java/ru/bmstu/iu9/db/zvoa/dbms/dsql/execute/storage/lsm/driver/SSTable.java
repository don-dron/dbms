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

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class SSTable<K extends Key, V extends Value> {
    private final File file;

    public SSTable(String path) throws DataStorageException {
        try {
            file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public void put(K key, V value) throws DataStorageException {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);

            Record<K, V>[] newRecords;

            if (fileInputStream.available() != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                Record<K, V>[] records = (Record<K, V>[]) objectInputStream.readObject();
                objectInputStream.close();

                boolean found = false;
                int index = -1;

                for (int i = 0; i < records.length; i++) {
                    int result = records[i].getKey().compareTo(key);
                    if (result < 0) {
                        continue;
                    } else if (result > 0) {
                        index = i;
                        break;
                    } else {
                        found = true;
                        index = i;
                        break;
                    }
                }

                Record<K, V> record = new Record<>(key, value);
                if (found) {
                    records[index] = record;
                    newRecords = records;
                } else {
                    newRecords = new Record[records.length + 1];

                    if (index == -1) {
                        System.arraycopy(records, 0, newRecords, 0, records.length);
                        newRecords[records.length] = record;
                    } else {
                        System.arraycopy(records, 0, newRecords, 0, index);
                        newRecords[index] = record;
                        System.arraycopy(records, index, newRecords, index + 1, records.length - index);
                    }
                }
            } else {
                newRecords = new Record[1];
                newRecords[0] = new Record(key, value);
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(newRecords);
            objectOutputStream.close();
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public void putAll(Map<K, V> output) throws DataStorageException {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);

            Record[] newRecords;

            if (fileInputStream.available() != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                Record<K, V>[] records = (Record<K, V>[]) objectInputStream.readObject();
                objectInputStream.close();
                newRecords = Arrays.stream(records).collect(Collectors.toMap(
                        Record::getKey,
                        Record::getValue,
                        (a, b) -> a,
                        () -> new TreeMap<>(output)))
                        .entrySet()
                        .stream()
                        .map(kvEntry -> new Record<>(kvEntry.getKey(), kvEntry.getValue()))
                        .toArray(Record[]::new);
            } else {
                newRecords = output.entrySet()
                        .stream()
                        .map(kvEntry -> new Record<>(kvEntry.getKey(), kvEntry.getValue()))
                        .toArray(Record[]::new);
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(newRecords);
            objectOutputStream.close();
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public V readKey(K key) throws DataStorageException {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);

            if (fileInputStream.available() != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                Record<K, V>[] records = (Record<K, V>[]) objectInputStream.readObject();
                objectInputStream.close();
                int index = Arrays.binarySearch(records, key);
                return index == -1 ? null : records[index].getValue();
            } else {
                return null;
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Map<K, V> readAllKeys() throws DataStorageException {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            if (fileInputStream.available() != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                Record<K, V>[] records = (Record<K, V>[]) objectInputStream.readObject();
                objectInputStream.close();
                return Arrays.stream(records).collect(Collectors.toMap(
                        Record::getKey,
                        Record::getValue,
                        (a, b) -> a,
                        TreeMap::new));
            } else {
                return new TreeMap<>();
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Map<K, V> readKeysByNumbers(int start, int end) throws DataStorageException {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            if (fileInputStream.available() != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                Record<K, V>[] records = (Record<K, V>[]) objectInputStream.readObject();
                objectInputStream.close();
                return Arrays.stream(records).skip(start).limit(end - start).collect(Collectors.toMap(
                        Record::getKey,
                        Record::getValue,
                        (a, b) -> a,
                        TreeMap::new));
            } else {
                return new TreeMap<>();
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Map<K, V> readKeyBlock(K startKey, int blockSize) {
        return null;
    }
}
