package ru.bmstu.iu9.db.zvoa.dbms.storage;

import org.junit.jupiter.api.Test;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver.LsmFileTree;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LsmFileTreeTest {
    public static final String path = "test_file";

    public String readFile(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }

    @Test
    public void simpleWriteReadTest() throws DataStorageException, IOException {
        LsmFileTree<TestKey, TestValue> lsmFileTree = new LsmFileTree<>(path);

        lsmFileTree.put(new TestKey(1), new TestValue("Andrey", 20));
        assertEquals(lsmFileTree.readKey(new TestKey(1)).name, "Andrey");
    }

    @Test
    public void writeTest() throws DataStorageException, IOException {
        LsmFileTree<TestKey, TestValue> lsmFileTree = new LsmFileTree<>(path);

        lsmFileTree.put(new TestKey(1), new TestValue("Andrey", 20));
        lsmFileTree.put(new TestKey(4), new TestValue("John", 16));
        lsmFileTree.put(new TestKey(3), new TestValue("Max", 4));
        lsmFileTree.put(new TestKey(2), new TestValue("Alex", 76));

        assertEquals(lsmFileTree.readKey(new TestKey(1)).name, "Andrey");
        assertEquals(lsmFileTree.readKey(new TestKey(2)).name, "Alex");
        assertEquals(lsmFileTree.readKey(new TestKey(3)).name, "Max");
        assertEquals(lsmFileTree.readKey(new TestKey(4)).name, "John");
    }

    @Test
    public void readAllTest() throws DataStorageException, IOException {
        LsmFileTree<TestKey, TestValue> lsmFileTree = new LsmFileTree<>(path);

        lsmFileTree.put(new TestKey(1), new TestValue("Andrey", 20));
        lsmFileTree.put(new TestKey(4), new TestValue("John", 16));
        lsmFileTree.put(new TestKey(3), new TestValue("Max", 4));
        lsmFileTree.put(new TestKey(2), new TestValue("Alex", 76));

        Map<TestKey,TestValue> map = lsmFileTree.readAllKeys();

        assertTrue(map.containsKey(new TestKey(1)));
        assertTrue(map.containsKey(new TestKey(2)));
        assertTrue(map.containsKey(new TestKey(3)));
        assertTrue(map.containsKey(new TestKey(4)));
    }

    public static class TestValue implements Value {
        public String name;
        public Integer age;

        public TestValue() {
        }

        public TestValue(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public Integer getAge() {
            return age;
        }

        public String getName() {
            return name;
        }
    }
}
