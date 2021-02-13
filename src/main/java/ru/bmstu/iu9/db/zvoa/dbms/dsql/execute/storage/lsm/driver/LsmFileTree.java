package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LsmFileTree<K extends Key, V extends Value> extends AbstractDbModule {

    private final File directory;
    private final LsmFile file;

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
}
