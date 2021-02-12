package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

public abstract class Schema implements Value {
    private final String schemaName;
    private final String schemaPath;

    public Schema(String schemaName, String schemaPath) {
        assert (schemaName != null);
        this.schemaName = schemaName;
        this.schemaPath = schemaPath == null ? schemaName : schemaPath;
    }

    public abstract boolean addTable(Table table) throws DataStorageException;

    public abstract Table getTable(String tableName) throws DataStorageException;

    public abstract Table useTable(String tableName) throws DataStorageException;

    public String getSchemaPath() {
        return schemaPath;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public static class SchemeIdentification implements Key {
        private String name;

        public SchemeIdentification(String name) {
            this.name = name;
        }

        @Override
        public int compareTo(Object o) {
            return name.compareTo(((SchemeIdentification) o).name);
        }
    }
}
