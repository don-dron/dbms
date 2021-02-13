package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

public class LsmFile<K extends Key, V extends Value> {
    private File file;

    public LsmFile(String path) throws IOException {
        file = new File(path);
        if (!file.exists()) {
            file.createNewFile();
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
            exception.printStackTrace();
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
                return Collections.emptyMap();
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
                return Collections.emptyMap();
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Map<K, V> readKeyBlock(K startKey, int blockSize) {
        return null;
    }
}
