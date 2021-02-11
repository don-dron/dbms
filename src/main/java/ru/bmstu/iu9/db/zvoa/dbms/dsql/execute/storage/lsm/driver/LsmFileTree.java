package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LsmFileTree<K extends Key, V extends Value> extends AbstractDbModule {

    private final File file;

    public LsmFileTree(String path) throws DataStorageException {
        try {
            this.file = new File(path);

            if (!file.exists()) {
                file.createNewFile();
            }
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
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            List<Record> records = new ArrayList<>();
            if (fileInputStream.available() != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);

                boolean inserted = false;
                while (true) {
                    Record<K, V> record = readNextObject(objectInputStream);

                    if (record != null) {
                        if (record.getKey().compareTo(key) == 0) {
                            records.add(new Record<>(key, value));
                            inserted = true;
                        } else if (record.getKey().compareTo(key) < 0) {
                            records.add(record);
                            records.add(new Record<>(key, value));
                            inserted = true;
                        } else {
                            records.add(record);
                        }
                    } else {
                        if (!inserted) {
                            records.add(new Record<>(key, value));
                        }

                        break;
                    }
                }
                objectInputStream.close();
            } else {
                records.add(new Record<>(key, value));
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);

            for (Record record : records) {
                objectOutputStream.writeObject(record);
            }

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
                Record<K, V> found = null;
                while (true) {
                    Record<K, V> record = readNextObject(objectInputStream);

                    if (record != null) {
                        if (record.getKey().compareTo(key) == 0) {
                            found = record;
                        }
                    } else {
                        break;
                    }
                }
                objectInputStream.close();
                return found != null ? found.getValue() : null;
            } else {
                return null;
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Map<K, V> readAllKeys() throws DataStorageException {
        Map<K, V> map = new TreeMap<>();
        Map<K, V> buffer;
        int start = 0;
        int step = 10;
        while (true) {
            buffer = readKeysByNumbers(start, start + step);
            start += step;

            if (buffer != null && !buffer.isEmpty()) {
                map.putAll(buffer);
            } else {
                break;
            }
        }

        return map;
    }

    public Map<K, V> readKeysByNumbers(int start, int end) throws DataStorageException {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            Map<K, V> map = new TreeMap<>();
            if (fileInputStream.available() != 0) {
                int index = 0;
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                while (true) {
                    if(index >= end) {
                        break;
                    }

                    Record<K, V> record = readNextObject(objectInputStream);
                    if (record != null) {
                        if (start <= index && index < end) {
                            map.put(record.getKey(), record.getValue());
                        }

                        index++;
                    } else {
                        break;
                    }
                }
                objectInputStream.close();
                return map;
            } else {
                return null;
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Record readNextObject(ObjectInputStream objectInputStream) {
        Record record;
        try {
            record = (Record<K, V>) objectInputStream.readObject();
        } catch (Exception exception) {
            record = null;
        }
        return record;
    }

    public Map<K, V> readKeyBlock(K startKey, int blockSize) {
        return null;
    }
}
