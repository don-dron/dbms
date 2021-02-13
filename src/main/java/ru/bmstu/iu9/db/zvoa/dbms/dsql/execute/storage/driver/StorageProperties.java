package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver;

import java.util.Map;
import java.util.function.Supplier;

public class StorageProperties<K, V> {
    private String path;
    private String name;

    public StorageProperties(String name, String path) {
        this.name = name;
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public enum StorageType {
        ROOT,
        SCHEMA,
        TABLE
    }
}
