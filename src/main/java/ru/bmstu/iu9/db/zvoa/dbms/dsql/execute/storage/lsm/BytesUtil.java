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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class BytesUtil {
    public static byte[] primitiveToBytes(Object object) {
        if (object instanceof Integer) {
            return intToBytes((Integer) object);
        } else if (object instanceof String) {
            return stringToBytes((String) object);
        } else if (object instanceof Long) {
            return longToBytes((Long) object);
        } else {
            throw new RuntimeException();
        }
    }

    public static byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }

    public static byte[] stringToBytes(String string) {
        byte[] bytes = string.getBytes();
        byte[] lenBytes = intToBytes(string.length());

        byte[] result = new byte[bytes.length + lenBytes.length];

        System.arraycopy(lenBytes, 0, result, 0, lenBytes.length);
        System.arraycopy(bytes, 0, result, lenBytes.length, bytes.length);
        return result;
    }

    public static int bytesToInt(byte[] bytes, int offset) {
        return (bytes[0 + offset] << 24) & 0xff000000 |
                (bytes[1 + offset] << 16) & 0x00ff0000 |
                (bytes[2 + offset] << 8) & 0x0000ff00 |
                (bytes[3 + offset] << 0) & 0x000000ff;
    }

    public static byte[] longToBytes(long l) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }

    public static long bytesToLong(final byte[] b, int offset) {
        long result = 0;
        for (int i = offset; i < offset + Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    public static String bytesToString(byte[] bytes, int offset) {
        int length = bytesToInt(bytes, offset);
        return new String(bytes, offset + 4, length);
    }

    public static byte[] listObjectsToBytes(List<Object> values) {
        List<byte[]> lst = values.stream().map(BytesUtil::primitiveToBytes).collect(Collectors.toList());

        byte[] allByteArray = new byte[lst.stream().map(x -> x.length).reduce(Integer::sum).orElse(0)];

        ByteBuffer buff = ByteBuffer.wrap(allByteArray);
        for (byte[] part : lst) {
            buff.put(part);
        }

        return buff.array();
    }

    public static List<Object> listFromBytes(byte[] bytes, int offset, List<Type> types) {
        List<Object> objects = new ArrayList<>();
        int local = offset;

        for (Type type : types) {
            if (type.equals(Type.INTEGER)) {
                objects.add(BytesUtil.bytesToInt(bytes, local));
                local += 4;
            } else if (type.equals(Type.STRING)) {
                String s = BytesUtil.bytesToString(bytes, local);
                objects.add(s);
                local += 4 + s.getBytes().length;
            } else if (type.equals(Type.LONG)) {
                Long s = BytesUtil.bytesToLong(bytes, local);
                objects.add(s);
                local += 8;
            } else {
                throw new IllegalArgumentException("WSADSD");
            }
        }

        return objects;
    }
}
