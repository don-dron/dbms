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

import org.jetbrains.annotations.NotNull;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Value;

import java.util.Objects;

/**
 * Record for LSM Tree.
 *
 * @param <K> type of keys
 * @param <V> type of values
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class Record<K extends Key, V extends Value> implements Comparable {
    private final K key;
    private final V value;
    private final long timestamp;

    /**
     * Recover record from file
     *
     * @param key       - key
     * @param value     - value
     * @param timestamp - in file timestamp
     */
    public Record(K key, V value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    /**
     * Create new record for lsm storage.
     *
     * @param key   - key
     * @param value - value
     */
    public Record(K key, V value) {
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Gets timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Gets value.
     *
     * @return the value
     */
    public V getValue() {
        return value;
    }

    /**
     * Gets key.
     *
     * @return the key
     */
    public K getKey() {
        return key;
    }

    @Override
    public int compareTo(@NotNull Object object) {
        if (object instanceof Key) {
            return key.compareTo(((Key) object));
        } else if (object instanceof Record) {
            return key.compareTo(((Record) object).getKey());
        } else {
            throw new IllegalArgumentException("Not comparable key");
        }
    }

    @Override
    public String toString() {
        return "Record{" +
                "key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Record)) return false;
        return compareTo((Record) o) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, timestamp);
    }
}
