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

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.BytesUtil;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class LsmLogger<K extends Key, V extends Value> {
    private final File file;
    private final ByteConverter<K, V> byteConverter;
    private final Map<K, V> oldData;
    private FileOutputStream fileOutputStream;
    private BufferedOutputStream bos;
    private int count;

    public LsmLogger(ByteConverter<K, V> byteConverter, String path) throws DataStorageException {
        try {
            this.byteConverter = byteConverter;
            this.file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
                oldData = new TreeMap<>();
                count = 0;
            } else {
                oldData = readLog();
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    private synchronized TreeMap<K, V> readLog() {
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            if (fileInputStream.available() > 0) {
                BufferedInputStream bis = new BufferedInputStream(fileInputStream);
                byte[] bytes = bis.readAllBytes();
                int offset = 0;

                TreeMap<K, V> treeMap = new TreeMap<>();

                while (offset < bytes.length) {
                    count++;

                    int keyLength = BytesUtil.bytesToInt(bytes, offset);
                    offset += 4;

                    K k = byteConverter.bytesToKey(bytes, offset);
                    offset += keyLength;

                    int valueLength = BytesUtil.bytesToInt(bytes, offset);
                    offset += 4;

                    V v = byteConverter.bytesToValue(bytes, offset);
                    offset += valueLength;

                    count++;
                    treeMap.put(k, v);
                }

                return treeMap;
            } else {
                return new TreeMap<>();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return new TreeMap<>();
        }
    }

    public int getCount() {
        return count;
    }

    public synchronized void put(K key, V value) {
        try {
            fileOutputStream = new FileOutputStream(file, true);
            bos = new BufferedOutputStream(fileOutputStream);
            byte[] keyBytes = byteConverter.keyToBytes(key);
            byte[] valueToBytes = byteConverter.valueToBytes(value);
            int allocate = 4 + 4 + keyBytes.length + valueToBytes.length;

            byte[] allByteArray = new byte[allocate];
            ByteBuffer buff = ByteBuffer.wrap(allByteArray);

            buff.putInt(keyBytes.length);
            buff.put(keyBytes);
            buff.putInt(valueToBytes.length);
            buff.put(valueToBytes);

            count++;

            bos.write(buff.array());
            bos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void delete() {
        file.delete();
    }
}
