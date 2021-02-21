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
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;

import java.util.List;

/**
 * Class for converting keys and values to bytes and back.
 *
 * @param <K> - type of keys
 * @param <V> - type of values
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public interface ByteConverter<K extends Key, V extends Value> {
    /**
     * Returns list of types of key objects.
     *
     * @return list of types
     */
    public @NotNull List<Type> getKeyTypes();

    /**
     * Returns list of types of value objects.
     *
     * @return list of types
     */
    public @NotNull List<Type> getValueTypes();

    /**
     * Convert bytes beginning with offset to key.
     *
     * @param bytes  - input bytes
     * @param offset - start offset
     * @return - new key
     */
    public @NotNull K bytesToKey(@NotNull byte[] bytes, int offset);

    /**
     * Convert bytes beginning with offset to value.
     *
     * @param bytes  - input bytes
     * @param offset - start offset
     * @return - new value
     */
    public @NotNull V bytesToValue(@NotNull byte[] bytes, int offset);

    /**
     * Convert key to bytes byte's array.
     *
     * @param key - input key
     * @return generated array
     */
    public @NotNull byte[] keyToBytes(@NotNull K key);

    /**
     * Convert value to bytes byte's array.
     *
     * @param value - input value
     * @return generated array
     */
    public @NotNull byte[] valueToBytes(@NotNull V value);
}

