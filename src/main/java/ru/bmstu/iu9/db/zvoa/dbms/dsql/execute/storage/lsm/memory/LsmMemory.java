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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.LsmCacheAlgorithm;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LsmMemory<K extends Key, V extends Value> extends AbstractDbModule implements IKeyValueStorage<K, V> {

    private Map<K, V> map;

    public int size() {
        return map.size();
    }

    @Override
    public V put(K key, V value) throws DataStorageException {
        return map.put(key, value);
    }

    @Override
    public V get(K key) throws DataStorageException {
        return map.get(key);
    }

    @Override
    public Map<K, V> getValues(Predicate<Map.Entry<K, V>> predicate) throws DataStorageException {
        Map<K, V> result = map.entrySet().stream().filter(predicate).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (a, b) -> a,
                TreeMap::new));
        return result;
    }

    public void updateCache(LsmCacheAlgorithm lsmCacheAlgorithm, Map<K, V> newData) {
        map = lsmCacheAlgorithm.swapMemory(map, newData);
    }

    public Map<K, V> swapMemory() {
        Map<K, V> oldMap = map;
        map = new TreeMap<>();
        return oldMap;
    }

    @Override
    public void init() throws DataStorageException, IOException {
        map = new TreeMap<>();
    }

    @Override
    public void run() {

    }

    @Override
    public void close() throws Exception {

    }
}
