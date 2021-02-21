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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter;

import org.jetbrains.annotations.NotNull;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.TypeException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class for work with bytes.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public final class BytesUtil {
    /**
     * Convert primitive object to bytes
     *
     * @param object - primitive object
     * @return bytes
     */
    public static @NotNull byte[] primitiveToBytes(@NotNull Object object) {
        if (object instanceof Integer) {
            return intToBytes((Integer) object);
        } else if (object instanceof String) {
            return stringToBytes((String) object);
        } else if (object instanceof Long) {
            return longToBytes((Long) object);
        } else {
            throw new TypeException("This type unsupported " + object);
        }
    }

    /**
     * Convert int to byte's array
     *
     * @param value - int value
     * @return bytes
     */
    public static @NotNull byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }

    /**
     * Convert string to byte's array
     *
     * @param value - string value
     * @return bytes
     */
    public static byte[] stringToBytes(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] lenBytes = intToBytes(value.length());

        byte[] result = new byte[bytes.length + lenBytes.length];

        System.arraycopy(lenBytes, 0, result, 0, lenBytes.length);
        System.arraycopy(bytes, 0, result, lenBytes.length, bytes.length);
        return result;
    }

    /**
     * Build int from bytes
     *
     * @param bytes bytes
     * @param offset start offset
     * @return int value
     */
    public static int bytesToInt(byte[] bytes, int offset) {
        return (bytes[0 + offset] << 24) & 0xff000000 |
                (bytes[1 + offset] << 16) & 0x00ff0000 |
                (bytes[2 + offset] << 8) & 0x0000ff00 |
                (bytes[3 + offset] << 0) & 0x000000ff;
    }

    /**
     * Convert long to byte's array
     *
     * @param value - long value
     * @return bytes
     */
    public static byte[] longToBytes(long value) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= Byte.SIZE;
        }
        return result;
    }

    /**
     * Build long from bytes
     *
     * @param bytes bytes
     * @param offset start offset
     * @return long value
     */
    public static long bytesToLong(byte[] bytes, int offset) {
        long result = 0;
        for (int i = offset; i < offset + Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }

    /**
     * Build string from bytes
     *  int   bytes blob
     * len | string
     *
     * @param bytes bytes
     * @param offset start offset
     * @return long value
     */
    public static String bytesToString(byte[] bytes, int offset) {
        int length = bytesToInt(bytes, offset);
        return new String(bytes, offset + 4, length, StandardCharsets.UTF_8);
    }

    /**
     * Build bytes for object's list
     *
     * @param values object's list
     * @param types type's object's
     * @return byte's blob
     */
    public static byte[] listObjectsToBytes(List<Object> values, List<Type> types) {
        List<byte[]> lst = values.stream().map(BytesUtil::primitiveToBytes).collect(Collectors.toList());

        byte[] allByteArray = new byte[lst.stream().map(x -> x.length).reduce(Integer::sum).orElse(0)];

        ByteBuffer buff = ByteBuffer.wrap(allByteArray);
        for (byte[] part : lst) {
            buff.put(part);
        }

        return buff.array();
    }

    /**
     * List from bytes by types
     *
     * @param bytes byte's blob
     * @param offset start offset
     * @param types object's types
     * @return list of object's
     */
    public static List<Object> listFromBytes(byte[] bytes, int offset, List<Type> types) {
        List<Object> objects = new ArrayList<>();
        int local = offset;

        for (Type type : types) {
            if (type.equals(Type.INTEGER)) {
                objects.add(bytesToInt(bytes, local));
                local += 4;
            } else if (type.equals(Type.STRING)) {
                String s = bytesToString(bytes, local);
                objects.add(s);
                local += 4 + s.getBytes(StandardCharsets.UTF_8).length;
            } else if (type.equals(Type.LONG)) {
                Long s = bytesToLong(bytes, local);
                objects.add(s);
                local += 8;
            } else {
                throw new TypeException("Unsupported type " + type);
            }
        }

        return objects;
    }

    private BytesUtil() {
    }
}
