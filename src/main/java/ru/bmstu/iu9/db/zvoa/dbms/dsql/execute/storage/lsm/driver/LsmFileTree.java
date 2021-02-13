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
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class LsmFileTree<K extends Key, V extends Value> extends AbstractDbModule {

    private final File directory;
    private final LsmFile<K, V> file;

    public LsmFileTree(String path) throws DataStorageException {
        try {
            this.directory = new File(path);
            if (!directory.exists()) {
                directory.mkdir();
            }

            file = new LsmFile(path + "/file");
        } catch (Exception exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    @Override
    public void init() throws DataStorageException, IOException {

    }

    @Override
    public void run() {

    }

    @Override
    public void close() throws Exception {

    }

    public void put(K key, V value) throws DataStorageException {
        file.put(key, value);
    }

    public V readKey(K key) throws DataStorageException {
        return file.readKey(key);
    }

    public Map<K, V> readAllKeys() throws DataStorageException {
        return file.readAllKeys();
    }

    public Map<K, V> readKeysByNumbers(int start, int end) throws DataStorageException {
        return file.readKeysByNumbers(start, end);
    }

    public Map<K, V> readKeyBlock(K startKey, int blockSize) {
        return null;
    }
}
