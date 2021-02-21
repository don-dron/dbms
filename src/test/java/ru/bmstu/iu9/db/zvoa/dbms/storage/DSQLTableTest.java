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
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.StorageProperties;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.lsm.LSMStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.TableConverter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.InsertSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.SelectSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DSQLTableTest {
    public static final String path = "test_file";

    @BeforeEach
    @AfterEach
    public void deleteFile() {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void writeTest() throws DataStorageException, IOException {
        DSQLTable table = DSQLTable.Builder.newBuilder()
                .setName("Table")
                .setTypes(Arrays.asList(Type.INTEGER, Type.STRING, Type.INTEGER))
                .setRowToKey(0)
                .build();
        LSMStorage<Key, Row> storage = new LSMStorage<>(new StorageProperties(new TableConverter(table,
                Arrays.asList(table.getKeyType()),
                table.getTypes()), "table", path));

        table.setStorage(storage);
        storage.init();

        table.insertRows(InsertSettings.Builder.newBuilder()
                .setRows(Arrays.asList(
                        Arrays.asList(1, "Andrey", 20),
                        Arrays.asList(4, "John", 16),
                        Arrays.asList(3, "Max", 4),
                        Arrays.asList(2, "Alex", 76)
                ))
                .build());

        storage.pushToDrive();

        List<Row> rows = table.selectRows(SelectSettings.Builder.newBuilder()
                .build());

        assertEquals(rows.get(0).getValues().get(1), "Andrey");
        assertEquals(rows.get(1).getValues().get(1), "Alex");
        assertEquals(rows.get(2).getValues().get(1), "Max");
        assertEquals(rows.get(3).getValues().get(1), "John");
    }

    @Test
    public void selectLastChangesTest() throws DataStorageException, IOException {
        DSQLTable table = DSQLTable.Builder.newBuilder()
                .setName("Table")
                .setRowToKey(0)
                .setTypes(Arrays.asList(Type.INTEGER, Type.STRING, Type.INTEGER))
                .build();
        LSMStorage<Key, Row> storage = new LSMStorage<>(new StorageProperties(new TableConverter(table,
                Arrays.asList(table.getKeyType()),
                table.getTypes()), "table", path));

        table.setStorage(storage);

        storage.init();

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
