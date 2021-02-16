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

public class SSTable<K extends Key, V extends Value> {
    private final File file;
    private final int level;
    private final int index;
    private Meta meta;

    public SSTable(String path, int level, int index) throws DataStorageException {
        try {
            file = new File(path + "/" + "sstable_" + level + "_" + index);
            this.index = index;
            this.level = level;
            if (!file.exists()) {
                file.createNewFile();
            } else {
                try {
                    FileInputStream fileInputStream = new FileInputStream(file);
                    ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                    meta = (Meta<K, V>) objectInputStream.readObject();
                    objectInputStream.close();
                } catch (Exception exception) {
                    throw new DataStorageException(exception.getMessage());
                }
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public int getIndex() {
        return index;
    }

    public int getLevel() {
        return level;
    }

    public File getFile() {
        return file;
    }

    public void putAll(Record<K, V>[] output) throws DataStorageException {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);

            meta = new Meta(Arrays.stream(output).map(Record::getKey).toArray(Key[]::new));

            objectOutputStream.writeObject(meta);
            objectOutputStream.writeObject(output);
            objectOutputStream.close();
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Record<K, V>[] readAllRecords() throws DataStorageException {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            if (fileInputStream.available() != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                meta = (Meta<K, V>) objectInputStream.readObject();
                Record<K, V>[] records = (Record<K, V>[]) objectInputStream.readObject();
                objectInputStream.close();
                return records;
            } else {
                return new Record[0];
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public void delete() {
        file.delete();
    }
}
