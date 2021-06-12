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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter;

import org.jetbrains.annotations.NotNull;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.DefaultKey;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.Arrays;
import java.util.List;

/**
 * Byte converter for converting Rows in tables.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class TableConverter implements ByteConverter<DefaultKey, Row> {
    private final Table table;
    private final List<Type> keyTypes;
    private final List<Type> valueTypes;

    /**
     * Constructor.
     *
     * @param table      - current table
     * @param keyTypes   - key types
     * @param valueTypes - value types
     */
    public TableConverter(@NotNull Table table, @NotNull List<Type> keyTypes, @NotNull List<Type> valueTypes) {
        this.table = table;
        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;
    }

    @Override
    public @NotNull List<Type> getKeyTypes() {
        return keyTypes;
    }

    @Override
    public @NotNull List<Type> getValueTypes() {
        return valueTypes;
    }

    @Override
    public @NotNull Row bytesToValue(byte[] bytes, int offset) {
        if (bytes.length == 0) {
            return Row.MARKER;
        }
        List<Object> objects = BytesUtil.listFromBytes(bytes, offset, valueTypes);
        return new Row(table, objects);
    }

    @Override
    public @NotNull DefaultKey bytesToKey(byte @NotNull [] bytes, int offset) {
        List<Object> objects = BytesUtil.listFromBytes(bytes, offset, keyTypes);
        return new DefaultKey(keyTypes.get(0), (Comparable) objects.get(0));
    }

    @Override
    public byte @NotNull [] keyToBytes(@NotNull DefaultKey key) {
        return BytesUtil.listObjectsToBytes(Arrays.asList(key.getComparable()), keyTypes);
    }

    @Override
    public byte @NotNull [] valueToBytes(@NotNull Row value) {
        if (value == Row.MARKER) {
            return new byte[0];
        }
        return BytesUtil.listObjectsToBytes(value.getValues(), valueTypes);
    }

    @Override
    public String toString() {
        return "TableConverter{" +
                "table=" + table +
                ", keyTypes=" + keyTypes +
                ", valueTypes=" + valueTypes +
                "}";
    }
}
