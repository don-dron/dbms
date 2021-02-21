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
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.ReadFromFileException;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Value;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.WriteToFileException;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.ByteConverter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.BytesUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

/**
 * Class for working with lof file for LSM tree storage. Before writing record to LSM Storage, we should write this
 * record to log file in the mode of appending.
 * <p>
 * https://howtodoinjava.com/java/io/java-append-to-file/
 * <p>
 * File can be deleted after pushing data to storage.
 *
 * @param <K> type of keys
 * @param <V> type of value
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class LSMLogfile<K extends Key, V extends Value> {
    private final File file;
    private final ByteConverter<K, V> byteConverter;
    private final Map<K, V> lastData;
    private int count;

    /**
     * Constructor. After creating load last data(if last launch crashed).
     *
     * @param byteConverter - byte converter for converting keys and values to/from bytes
     * @param path          - mount path for log file
     */
    public LSMLogfile(@NotNull ByteConverter<K, V> byteConverter, @NotNull String path) {
        this.byteConverter = byteConverter;
        this.file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException exception) {
                throw new LSMFileCreatingException("Cannot create log file for path "
                        + path + " : " + Throwables.getStackTraceAsString(exception));
            }
            lastData = new TreeMap<>();
            count = 0;
        } else {
            lastData = readLog();
        }
    }

    /**
     * Returns current record count
     *
     * @return current count
     */
    public synchronized int getCount() {
        return count;
    }

    /**
     * Returns data logged before launch DBMS. Not empty if last launch crashed.
     *
     * @return - last data
     */
    public synchronized @NotNull Map<K, V> getLastData() {
        return lastData;
    }

    /**
     * Append key and value to log file.
     *
     * @param key   - key for writing
     * @param value - value for writing
     */
    public synchronized void put(@NotNull K key, @NotNull V value) {
        try {
            // Append mode
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            BufferedOutputStream bos = new BufferedOutputStream(fileOutputStream);
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
        } catch (IOException ioException) {
            throw new WriteToFileException("Cannot append row to logfile: " + Throwables.getStackTraceAsString(ioException));
        }
    }

    /**
     * Delete useless log file.
     */
    public synchronized void delete() {
        if (!file.delete()) {
            throw new LSMFileDeletingException("Cannot delete log file: " + file.getAbsolutePath());
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
        } catch (IOException ioException) {
            throw new ReadFromFileException("Cannot read logfile: " + Throwables.getStackTraceAsString(ioException));
        }
    }
}
