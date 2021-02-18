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
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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
//                    FileInputStream fileInputStream = new FileInputStream(file);
//                    ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
//                    meta = (Meta<K, V>) objectInputStream.readObject();
//                    objectInputStream.close();
                } catch (Exception exception) {
                    throw new DataStorageException(exception.getMessage());
                }
            }
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Meta getMeta() {
        return meta;
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

    /*

    (len)types

    start_data, key_num   - 8 bytes

    int (4) int (4)  long (8)
  0 offset, length, timestamp
  1 offset, length, timestamp
  2 offset, length, timestamp
    .........
    ....

    offset 0    offset 1
    v           v
    key, value, key, value..........
    ^  ^
    length 2
     */

    public void putAll(Record<K, V>[] records) throws DataStorageException {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fileOutputStream);

            byte[] keyType = null;

            if (records[0].getKey() instanceof TableIdentification) {
                keyType = BytesUtil.stringToBytes("TB");
            } else if (records[0].getKey() instanceof SchemeIdentification) {
                keyType = BytesUtil.stringToBytes("SC");
            } else {
                keyType = BytesUtil.stringToBytes("PR");
            }

            byte[] valueType = null;

            if (records[0].getValue() instanceof Table) {
                valueType = BytesUtil.stringToBytes("TB");
            } else if (records[0].getValue() instanceof Schema) {
                valueType = BytesUtil.stringToBytes("SC");
            } else {
                valueType = BytesUtil.stringToBytes("PR");
            }

            byte[] keyTypes = BytesUtil.stringToBytes(records[0]
                    .getKey()
                    .getTypes()
                    .stream()
                    .map(Enum::name)
                    .collect(Collectors.joining(",")));

            byte[] valueTypes = BytesUtil.stringToBytes(records[0]
                    .getValue()
                    .toObjects()
                    .stream()
                    .map(x -> {
                        if (x instanceof Integer) {
                            return "INTEGER";
                        } else if (x instanceof String) {
                            return "STRING";
                        } else if (x instanceof Long) {
                            return "LONG";
                        } else {
                            throw new IllegalArgumentException("sadasadasdssd");
                        }
                    })
                    .collect(Collectors.joining(",")));

            int size = records.length;
            int startData = 0 + 8 + keyType.length + valueType.length + keyTypes.length + valueTypes.length;
            int allocate = 0 + 8 + keyType.length + valueType.length + keyTypes.length + valueTypes.length;
            int currentOffset = 0;

            List<Integer> offsets = new ArrayList<>();
            List<Integer> lengths = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();

            for (Record record : records) {
                byte[] key = BytesUtil.listObjectsToBytes(record.getKey().toObjects());
                byte[] value = BytesUtil.listObjectsToBytes(record.getValue().toObjects());

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

            byte[] allByteArray = new byte[allocate];
            ByteBuffer buff = ByteBuffer.wrap(allByteArray);
            buff.put(keyType);
            buff.put(valueType);
            buff.put(keyTypes);
            buff.put(valueTypes);
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

            bos.write(buff.array());
            bos.close();
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new DataStorageException(exception.getMessage());
        }
    }

    public Record<K, V>[] readAllRecords() throws DataStorageException {
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            if (fileInputStream.available() != 0) {
                BufferedInputStream bis = new BufferedInputStream(fileInputStream);
                byte[] bytes = bis.readAllBytes();

                int offset = 0;
                String keyType = BytesUtil.bytesToString(bytes, offset);
                offset += 4 + keyType.getBytes().length;

                String valueType = BytesUtil.bytesToString(bytes, offset);
                offset += 4 + valueType.getBytes().length;

                KeyBuilder keyBuilder = null;

                ValueBuilder valueBuilder = null;

                String stringKeyTypes = BytesUtil.bytesToString(bytes, offset);
                offset += 4 + stringKeyTypes.getBytes().length;

                String stringValueTypes = BytesUtil.bytesToString(bytes, offset);
                offset += 4 + stringValueTypes.getBytes().length;

                List<Type> keyTypes = Arrays.stream(stringKeyTypes.split(","))
                        .map(str -> {
                            if (str.equals("INTEGER")) {
                                return Type.INTEGER;
                            } else if (str.equals("STRING")) {
                                return Type.STRING;
                            } else if (str.equals("LONG")) {
                                return Type.LONG;
                            } else {
                                throw new IllegalArgumentException("sadasd");
                            }
                        }).collect(Collectors.toList());

                List<Type> valueTypes = Arrays.stream(stringValueTypes.split(","))
                        .map(str -> {
                            if (str.equals("INTEGER")) {
                                return Type.INTEGER;
                            } else if (str.equals("STRING")) {
                                return Type.STRING;
                            } else if (str.equals("LONG")) {
                                return Type.LONG;
                            } else {
                                throw new IllegalArgumentException("sadasd");
                            }
                        }).collect(Collectors.toList());

                if (keyType.equals("TB")) {
                    keyBuilder = TableIdentification::new;
                } else if (keyType.equals("SC")) {
                    keyBuilder = SchemeIdentification::new;
                } else {
                    keyBuilder = (lst) -> new DefaultKey(keyTypes.get(0), lst);
                }

                if (valueType.equals("TB")) {
                    valueBuilder = DSQLTable::new;
                } else if (valueType.equals("SC")) {
                    valueBuilder = DSQLSchema::new;
                } else {
                    valueBuilder = Row::new;
                }

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

                for (int i = 0; i < size; i++) {

                    offset = startData + offsets.get(i);

                    List<Object> objects = BytesUtil.listFromBytes(bytes, offset, keyTypes);
                    Key key = keyBuilder.apply(objects);

                    offset += lengths.get(i);

                    objects = BytesUtil.listFromBytes(bytes, offset, valueTypes);
                    Value value = valueBuilder.apply(objects);

                    Record record = new Record(key, value, timestamps.get(i));
                    records.add(record);
                }

                return records.toArray(Record[]::new);
            } else {
                return new Record[0];
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new DataStorageException(exception.getMessage());
        }
    }

    public interface KeyBuilder extends Function<List<Object>, Key> {
    }

    public interface ValueBuilder extends Function<List<Object>, Value> {
    }

    public void delete() {
        file.delete();
    }
}
