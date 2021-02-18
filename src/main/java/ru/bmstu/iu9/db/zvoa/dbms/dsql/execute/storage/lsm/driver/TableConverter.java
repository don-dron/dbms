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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.BytesUtil;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.DefaultKey;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.Arrays;
import java.util.List;

public class TableConverter implements ByteConverter<DefaultKey, Row> {
    private Table table;
    private List<Type> keyTypes;
    private List<Type> valueTypes;

    public TableConverter(Table table, List<Type> keyTypes, List<Type> valueTypes) {
        this.table = table;
        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;
    }

    @Override
    public List<Type> getKeyTypes() {
        return keyTypes;
    }

    @Override
    public List<Type> getValueTypes() {
        return valueTypes;
    }

    @Override
    public Row bytesToValue(byte[] bytes, int offset) {
        List<Object> objects = BytesUtil.listFromBytes(bytes, offset, getValueTypes());
        return new Row(table, objects);
    }

    @Override
    public DefaultKey bytesToKey(byte[] bytes, int offset) {
        List<Object> objects = BytesUtil.listFromBytes(bytes, offset, getKeyTypes());
        return new DefaultKey(getKeyTypes().get(0), (Comparable) objects.get(0));
    }

    @Override
    public byte[] keyToBytes(DefaultKey key) {
        return BytesUtil.listObjectsToBytes(Arrays.asList(key.getComparable()), getKeyTypes());
    }

    @Override
    public byte[] valueToBytes(Row value) {
        return BytesUtil.listObjectsToBytes(value.getValues(), getValueTypes());
    }
}
