package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver;

public class StorageProperties {
    private String path;
    private String name;

    public StorageProperties(String name, String path) {
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
