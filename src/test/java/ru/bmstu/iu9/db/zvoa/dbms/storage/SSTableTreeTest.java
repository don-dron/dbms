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
package ru.bmstu.iu9.db.zvoa.dbms.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver.LsmFileTree;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SSTableTreeTest {
    public static final String path = "test_file";

    public String readFile(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }

    @AfterEach
    @BeforeEach
    public void deleteFile() {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void simpleWriteReadTest() throws DataStorageException, IOException {
        LsmFileTree<TestKey, TestValue> lsmFileTree = new LsmFileTree<>(path);

        Map<TestKey,TestValue> map = new TreeMap<>();
        map.put(new TestKey(1), new TestValue("Andrey", 20));
        lsmFileTree.putAll(map);
        assertEquals(lsmFileTree.readAllKeys().get(new TestKey(1)).name, "Andrey");
    }

    @Test
    public void writeTest() throws DataStorageException, IOException {
        LsmFileTree<TestKey, TestValue> lsmFileTree = new LsmFileTree<>(path);

        Map<TestKey,TestValue> map = new TreeMap<>();
        map.put(new TestKey(1), new TestValue("Andrey", 20));
        map.put(new TestKey(4), new TestValue("John", 16));
        map.put(new TestKey(3), new TestValue("Max", 4));
        map.put(new TestKey(2), new TestValue("Alex", 76));

        lsmFileTree.putAll(map);
        map = lsmFileTree.readAllKeys();
        assertEquals(map.get(new TestKey(1)).name, "Andrey");
        assertEquals(map.get(new TestKey(2)).name, "Alex");
        assertEquals(map.get(new TestKey(3)).name, "Max");
        assertEquals(map.get(new TestKey(4)).name, "John");
    }

    @Test
    public void readAllTest() throws DataStorageException, IOException {
        LsmFileTree<TestKey, TestValue> lsmFileTree = new LsmFileTree<>(path);

        Map<TestKey,TestValue> map = new TreeMap<>();
        map.put(new TestKey(1), new TestValue("Andrey", 20));
        map.put(new TestKey(4), new TestValue("John", 16));
        map.put(new TestKey(3), new TestValue("Max", 4));
        map.put(new TestKey(2), new TestValue("Alex", 76));

        lsmFileTree.putAll(map);
        map = lsmFileTree.readAllKeys();

        assertTrue(map.containsKey(new TestKey(1)));
        assertTrue(map.containsKey(new TestKey(2)));
        assertTrue(map.containsKey(new TestKey(3)));
        assertTrue(map.containsKey(new TestKey(4)));
    }
}
