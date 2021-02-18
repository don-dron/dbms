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
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver.LsmFileTree;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver.TableConverter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.DefaultKey;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SSTableTreeTest {
    public static final String path = "test_file";
    private Table table;

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
        table = DSQLTable.Builder.newBuilder()
                .setName("table")
                .setTypes(Arrays.asList(Type.STRING, Type.INTEGER))
                .build();
    }

    @Test
    public void simpleWriteReadTest() throws DataStorageException, IOException {
        LsmFileTree<DefaultKey, Row> lsmFileTree = new LsmFileTree<>(new TableConverter(table,
                Arrays.asList(Type.INTEGER),
                table.getTypes()), path);

        Map<DefaultKey, Row> map = new TreeMap<>();
        map.put(new DefaultKey(Type.INTEGER, 1), new Row(table, Arrays.asList("Andrey", 20)));
        lsmFileTree.putAll(map);
        assertEquals(lsmFileTree.readAllKeys().get(new DefaultKey(Type.INTEGER, 1)).getValues().get(0), "Andrey");
    }

    @Test
    public void writeTest() throws DataStorageException, IOException {
        LsmFileTree<DefaultKey, Row> lsmFileTree = new LsmFileTree<>(new TableConverter(table,
                Arrays.asList(Type.INTEGER),
                        table.getTypes()), path);

        Map<DefaultKey, Row> map = new TreeMap<>();
        map.put(new DefaultKey(Type.INTEGER, 1), new Row(table, Arrays.asList("Andrey", 20)));
        map.put(new DefaultKey(Type.INTEGER, 4), new Row(table, Arrays.asList("John", 16)));
        map.put(new DefaultKey(Type.INTEGER, 3), new Row(table, Arrays.asList("Max", 4)));
        map.put(new DefaultKey(Type.INTEGER, 2), new Row(table, Arrays.asList("Alex", 76)));

        lsmFileTree.putAll(map);
        map = lsmFileTree.readAllKeys();
        assertEquals(map.get(new DefaultKey(Type.INTEGER, 1)).getValues().get(0), "Andrey");
        assertEquals(map.get(new DefaultKey(Type.INTEGER, 2)).getValues().get(0), "Alex");
        assertEquals(map.get(new DefaultKey(Type.INTEGER, 3)).getValues().get(0), "Max");
        assertEquals(map.get(new DefaultKey(Type.INTEGER, 4)).getValues().get(0), "John");
    }

    @Test
    public void readAllTest() throws DataStorageException, IOException {
        LsmFileTree<DefaultKey, Row> lsmFileTree = new LsmFileTree<>(new TableConverter(table,
                Arrays.asList(Type.INTEGER),
                table.getTypes()), path);

        Map<DefaultKey, Row> map = new TreeMap<>();
        map.put(new DefaultKey(Type.INTEGER, 1), new Row(table, Arrays.asList("Andrey", 20)));
        map.put(new DefaultKey(Type.INTEGER, 4), new Row(table, Arrays.asList("John", 16)));
        map.put(new DefaultKey(Type.INTEGER, 3), new Row(table, Arrays.asList("Max", 4)));
        map.put(new DefaultKey(Type.INTEGER, 2), new Row(table, Arrays.asList("Alex", 76)));

        lsmFileTree.putAll(map);
        map = lsmFileTree.readAllKeys();

        assertTrue(map.containsKey(new DefaultKey(Type.INTEGER, 1)));
        assertTrue(map.containsKey(new DefaultKey(Type.INTEGER, 2)));
        assertTrue(map.containsKey(new DefaultKey(Type.INTEGER, 3)));
        assertTrue(map.containsKey(new DefaultKey(Type.INTEGER, 4)));
    }
}
