package ru.bmstu.iu9.db.zvoa.dbms.storage;

import org.junit.jupiter.api.Test;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.StorageProperties;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.LsmStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LSMStorageTest {
    public static final String path = "test_file";

    public String readFile(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }

    @Test
    public void simpleWriteReadTest() throws DataStorageException, IOException {
        LsmStorage<TestKey, TestValue> storage = new LsmStorage<>(new StorageProperties("table", path));
        storage.init();

        storage.put(new TestKey(1), new TestValue("Andrey", 20));
        System.out.println(storage.get(new TestKey(1)).name);
        System.out.println(readFile(path));
    }
}
