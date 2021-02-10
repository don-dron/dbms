package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class Table {
    private final String tableName;
    private final List<Type> types;

    public Table(String tableName, List<Type> types) {
        this.tableName = tableName;
        this.types = types;
    }

    public List<Type> getTypes() {
        return types;
    }

    public String getTableName() {
        return tableName;
    }

    public abstract List<Row> selectRows(SelectSettings selectSettings) throws DataStorageException;

    public abstract List<Row> insertRows(InsertSettings insertSettings) throws DataStorageException;

    public abstract List<Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;
        return tableName.equals(table.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName);
    }

    public static class Row {
        private Table table;
        private Object key;
        private List<Object> values;

        public Row(Table table, List<Object> values) {
            this.table = table;
            this.key = values.get(0);
            this.values = values;
        }

        public Object getKey() {
            return key;
        }

        public Table getTable() {
            return table;
        }

        public List<Object> getValues() {
            return values;
        }

        public static Row parseString(Table table, String rawString) {
            return new Row(table, Arrays.asList(rawString));
        }

        @Override
        public String toString() {
            return "Row{" +
                    "key=" + key +
                    ", values=" + values +
                    '}';
        }
    }
}
