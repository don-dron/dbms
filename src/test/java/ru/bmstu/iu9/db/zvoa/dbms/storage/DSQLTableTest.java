package ru.bmstu.iu9.db.zvoa.dbms.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.StorageProperties;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.LsmStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.InsertSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.SelectSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DSQLTableTest {
    public static final String path = "test_file";

    @BeforeEach
    public void deleteFile() {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void writeTest() throws DataStorageException, IOException {
        LsmStorage<Key, Row> storage = new LsmStorage<>(new StorageProperties("table", path));

        storage.init();

        DSQLTable table = DSQLTable.Builder.newBuilder()
                .setName("Table")
                .setRowToKey(row -> new TestKey((Integer) row.getValues().get(0)))
                .setStorage(storage)
                .build();

        table.insertRows(InsertSettings.Builder.newBuilder()
                .setRows(Arrays.asList(
                        Arrays.asList(1, "Andrey", 20),
                        Arrays.asList(4, "John", 16),
                        Arrays.asList(3, "Max", 4),
                        Arrays.asList(2, "Alex", 76)
                ))
                .build());

        storage.run();

        List<Row> rows = table.selectRows(SelectSettings.Builder.newBuilder()
                .build());

        assertEquals(rows.get(0).getValues().get(1), "Andrey");
        assertEquals(rows.get(1).getValues().get(1), "Alex");
        assertEquals(rows.get(2).getValues().get(1), "Max");
        assertEquals(rows.get(3).getValues().get(1), "John");
    }

    @Test
    public void selectLastChangesTest() throws DataStorageException, IOException {
        LsmStorage<Key, Row> storage = new LsmStorage<>(new StorageProperties("table", path));

        storage.init();

        DSQLTable table = DSQLTable.Builder.newBuilder()
                .setName("Table")
                .setRowToKey(row -> new TestKey((Integer) row.getValues().get(0)))
                .setStorage(storage)
                .build();
        // 1. Put rows (to memory)
        table.insertRows(InsertSettings.Builder.newBuilder()
                .setRows(Arrays.asList(
                        Arrays.asList(1, "Andrey", 20),
                        Arrays.asList(4, "John", 16),
                        Arrays.asList(3, "Max", 4),
                        Arrays.asList(2, "Alex", 76)
                ))
                .build());

        // Swap memory to drive
        storage.pushToDrive();

        // Assertions read
        List<Row> rows = table.selectRows(SelectSettings.Builder.newBuilder()
                .build());
        assertEquals(rows.get(0).getValues().get(1), "Andrey");
        assertEquals(rows.get(1).getValues().get(1), "Alex");
        assertEquals(rows.get(2).getValues().get(1), "Max");
        assertEquals(rows.get(3).getValues().get(1), "John");

        // 2. Put rows (to memory), but not swap data to drive
        table.insertRows(InsertSettings.Builder.newBuilder()
                .setRows(Arrays.asList(
                        Arrays.asList(3, "Paul", 4),
                        Arrays.asList(2, "Li", 76)
                ))
                .build());

        // Before memory swapping to drive
        rows = table.selectRows(SelectSettings.Builder.newBuilder()
                .build());

        // Checks that we read last data
        assertEquals(rows.get(0).getValues().get(1), "Andrey");
        assertEquals(rows.get(1).getValues().get(1), "Li");
        assertEquals(rows.get(2).getValues().get(1), "Paul");
        assertEquals(rows.get(3).getValues().get(1), "John");
    }
}
