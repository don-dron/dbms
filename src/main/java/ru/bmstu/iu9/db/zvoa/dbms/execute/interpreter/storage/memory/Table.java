package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class Table implements Value {
    private final String tableName;
    private final String tablePath;
    private final Function<Row, Key> rowKeyFunction;
    private final List<Type> types;

    public Table(String tableName,
                 String tablePath,
                 List<Type> types,
                 Function<Row, Key> rowKeyFunction) {
        this.tableName = tableName;
        this.types = types;
        this.rowKeyFunction = rowKeyFunction == null ? getDefaultKeyFunction() : rowKeyFunction;
        this.tablePath = tablePath == null ? tableName : tablePath;
    }

    public Function<Row, Key> getRowKeyFunction() {
        return rowKeyFunction;
    }

    public Function<Row, Key> getDefaultKeyFunction() {
        if (types == null || types.isEmpty()) {
            return new DefaultRowToKey();
        } else {
            Type type = types.get(0);

            if (type == Type.INTEGER) {
                return new LongRowToKey();
            } else {
                return new DefaultRowToKey();
            }
        }
    }

    public String getTablePath() {
        return tablePath;
    }

    public List<Type> getTypes() {
        return types;
    }

    public String getTableName() {
        return tableName;
    }

    public Key getRowKey(Row row) {
        return rowKeyFunction.apply(row);
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
}
