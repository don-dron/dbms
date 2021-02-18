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
package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class Table extends Value {
    private String tableName;
    private String tablePath;
    private Integer rowKeyFunction;
    private List<Type> types;

    public Table(String tableName,
                 String tablePath,
                 List<Type> types,
                 Integer rowKeyFunction) {
        this.tableName = tableName;
        this.types = types;
        this.rowKeyFunction = rowKeyFunction == null ? 0 : rowKeyFunction;
        this.tablePath = tablePath == null ? tableName : tablePath;
    }

    public Type getKeyType() {
        return types.get(rowKeyFunction);
    }

    public List<Type> getTypes() {
        return types;
    }

    public Integer getRowKeyFunction() {
        return rowKeyFunction;
    }

    public List<Type> getTableTypes() {
        return Arrays.asList(Type.STRING, Type.STRING, Type.INTEGER, Type.STRING);
    }

    public String getTablePath() {
        return tablePath;
    }

    public String getTableName() {
        return tableName;
    }

    public Key getRowKey(Row row) {
        return new DefaultKey(types.get(rowKeyFunction), (Comparable) row.getValues().get(rowKeyFunction));
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
