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

import com.google.common.base.Throwables;
import org.jetbrains.annotations.NotNull;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.BytesUtil;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * SSTable implementation. SSTable can be written only once, but read as many as you want.
 * <p>
 * SSTable format:
 * <p>
 * start_data, size   - 8 bytes
 * <p>
 * int (4) int (4)  long (8)
 * 0 offset, length, timestamp
 * 1 offset, length, timestamp
 * 2 offset, length, timestamp
 * .........
 * .........
 * <p>
 * offset 0    offset 1
 * v           v
 * key, value, key, value..........
 * ^  ^
 * length 0
 *
 * @param <K> - type of keys
 * @param <V> - type of values
 */
public class SSTable<K extends Key, V extends Value> {
    public static final String SSTABLE_FORMAT = "sstable_%d_%d";
    private final ByteConverter<K, V> byteConverter;
    private final File file;
    private final int level;
    private final int index;
    private Meta<K, V> meta;

    /**
     * Constructor for new SSTable.
     *
     * @param byteConverter - byte converter for converting keys and values to/from bytes
     * @param path          - mount path for SSTable
     * @param level         - current level in LSM File Tree
     * @param index         - index in LSM File Tree level
     */
    public SSTable(@NotNull ByteConverter<K, V> byteConverter, @NotNull String path, int level, int index) {
        try {
            file = new File(path + "/" + String.format(SSTABLE_FORMAT, level, index));
            this.index = index;
            this.level = level;
            this.byteConverter = byteConverter;

            if (!file.exists()) {
                file.createNewFile();
                meta = new Meta<>((K[]) new Key[0]);
            } else {
                meta = new Meta<>(readAllKey());
            }
        } catch (IOException ioException) {
            throw new LSMFileCreatingException("Cannot create SSTable file for path "
                    + path + " : " + Throwables.getStackTraceAsString(ioException));
        }
    }

    /**
     * Returns meta information about SSTable.
     *
     * @return meta information
     */
    public @NotNull Meta getMeta() {
        return meta;
    }

    /**
     * Gets index.
     *
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    /**
     * Gets level.
     *
     * @return the level
     */
    public int getLevel() {
        return level;
    }

    /**
     * Gets file.
     *
     * @return the file
     */
    public @NotNull File getFile() {
        return file;
    }

    /**
     * Write records to SSTable.
     *
     * @param records - records for writing
     */
    public void putAll(@NotNull Record<K, V>[] records) {
        try {
            int size = records.length;
            int startData = 0 + 8;
            int allocate = 0 + 8;
            int currentOffset = 0;

            List<Integer> offsets = new ArrayList<>();
            List<Integer> lengths = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();

            List<K> keyList = new ArrayList<>();

            for (Record<K, V> record : records) {
                byte[] key = byteConverter.keyToBytes(record.getKey());
                byte[] value = byteConverter.valueToBytes(record.getValue());

                keyList.add(record.getKey());

                offsets.add(currentOffset);
                startData += 4;
                allocate += 4;

                lengths.add(key.length);
                startData += 4;
                allocate += 4;

                timestamps.add(record.getTimestamp());
                startData += 8;
                allocate += 8;

                keys.add(key);
                allocate += key.length;

                values.add(value);
                allocate += value.length;

                currentOffset += key.length + value.length;
            }

            meta = new Meta<K, V>(keyList.toArray((int value) -> (K[]) new Key[value]));

            byte[] allByteArray = new byte[allocate];
            ByteBuffer buff = ByteBuffer.wrap(allByteArray);
            buff.putInt(startData);
            buff.putInt(size);

            for (int i = 0; i < keys.size(); i++) {
                buff.putInt(offsets.get(i));
                buff.putInt(lengths.get(i));
                buff.putLong(timestamps.get(i));
            }

            for (int i = 0; i < keys.size(); i++) {
                buff.put(keys.get(i));
                buff.put(values.get(i));
            }

            FileOutputStream fileOutputStream = new FileOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fileOutputStream);

            bos.write(buff.array());
            bos.close();
        } catch (IOException exception) {
            throw new WriteToFileException("Cannot write records to file: "
                    + Throwables.getStackTraceAsString(exception));
        }
    }

    /**
     * Read all keys from SSTable.
     *
     * @return key's array
     */
    public @NotNull K[] readAllKey() {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            if (fileInputStream.available() != 0) {
                BufferedInputStream bis = new BufferedInputStream(fileInputStream);
                byte[] bytes = bis.readAllBytes();
                bis.close();

                int offset = 0;
                int startData = BytesUtil.bytesToInt(bytes, offset);
                offset += 4;

                int size = BytesUtil.bytesToInt(bytes, offset);
                offset += 4;

                List<Integer> offsets = new ArrayList<>();

                for (int i = 0; i < size; i++) {
                    offsets.add(BytesUtil.bytesToInt(bytes, offset));
                    offset += 4;
                    offset += 4;
                    offset += 8;
                }

                List<K> keyList = new ArrayList<>();

                for (int i = 0; i < size; i++) {
                    offset = startData + offsets.get(i);

                    K key = byteConverter.bytesToKey(bytes, offset);
                    keyList.add(key);
                }

                return keyList.toArray((int value) -> (K[]) new Key[value]);
            } else {
                return (K[]) new Key[0];
            }
        } catch (IOException exception) {
            throw new ReadFromFileException("Cannot read keys from file: " + exception.getMessage());
        }
    }

    /**
     * Read all records(pairs key + value) from SSTable.
     *
     * @return records array
     */
    public @NotNull Record<K, V>[] readAllRecords() {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            if (fileInputStream.available() != 0) {
                BufferedInputStream bis = new BufferedInputStream(fileInputStream);
                byte[] bytes = bis.readAllBytes();
                bis.close();

                int offset = 0;
                int startData = BytesUtil.bytesToInt(bytes, offset);
                offset += 4;

                int size = BytesUtil.bytesToInt(bytes, offset);
                offset += 4;

                List<Integer> offsets = new ArrayList<>();
                List<Integer> lengths = new ArrayList<>();
                List<Long> timestamps = new ArrayList<>();
                List<Record> records = new ArrayList<>();

                for (int i = 0; i < size; i++) {
                    offsets.add(BytesUtil.bytesToInt(bytes, offset));
                    offset += 4;

                    lengths.add(BytesUtil.bytesToInt(bytes, offset));
                    offset += 4;

                    timestamps.add(BytesUtil.bytesToLong(bytes, offset));
                    offset += 8;
                }

                List<K> keyList = new ArrayList<>();

                for (int i = 0; i < size; i++) {

                    offset = startData + offsets.get(i);

                    K key = byteConverter.bytesToKey(bytes, offset);
                    keyList.add(key);

                    offset += lengths.get(i);

                    V value = byteConverter.bytesToValue(bytes, offset);

                    Record record = new Record(key, value, timestamps.get(i));
                    records.add(record);
                }

                meta = new Meta<K, V>(keyList.toArray((int value) -> (K[]) new Key[value]));
                return records.toArray(Record[]::new);
            } else {
                return new Record[0];
            }
        } catch (IOException exception) {
            throw new ReadFromFileException("Cannot read records from file: " + exception.getMessage());
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
}
