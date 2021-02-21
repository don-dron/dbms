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

import org.jetbrains.annotations.NotNull;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.BytesUtil;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.TableIdentification;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SchemeConverter implements ByteConverter<TableIdentification, Table> {
    @Override
    public @NotNull List<Type> getKeyTypes() {
        return Arrays.asList(Type.STRING);
    }

    @Override
    public @NotNull List<Type> getValueTypes() {
        return Arrays.asList(Type.STRING, Type.STRING, Type.INTEGER, Type.STRING);
    }

    @Override
    public @NotNull Table bytesToValue(byte[] bytes, int offset) {
        List<Object> objects = BytesUtil.listFromBytes(bytes, offset, getValueTypes());
        return new DSQLTable.Builder()
                .setName((String) objects.get(0))
                .setPath((String) objects.get(1))
                .setRowToKey((Integer) objects.get(2))
                .setTypes(Arrays.stream(((String) objects.get(3)).split(","))
                        .map(str -> {
                            if (str.equals("INTEGER")) {
                                return Type.INTEGER;
                            } else if (str.equals("STRING")) {
                                return Type.STRING;
                            } else if (str.equals("LONG")) {
                                return Type.LONG;
                            } else {
                                throw new IllegalArgumentException("asdasd");
                            }
                        })
                        .collect(Collectors.toList()))
                .build();
    }

    @Override
    public @NotNull TableIdentification bytesToKey(byte @NotNull [] bytes, int offset) {
        List<Object> objects = BytesUtil.listFromBytes(bytes, offset, getKeyTypes());
        return new TableIdentification((String) objects.get(0));
    }

    @Override
    public byte @NotNull [] keyToBytes(@NotNull TableIdentification key) {
        return BytesUtil.listObjectsToBytes(Arrays.asList(key.getTableName()), getKeyTypes());
    }

    @Override
    public byte @NotNull [] valueToBytes(@NotNull Table value) {
        return BytesUtil.listObjectsToBytes(Arrays.asList(
                value.getTableName(),
                value.getTablePath(),
                value.getRowKeyFunction(),
                value.getTypes().stream().map(Enum::name).collect(Collectors.joining(","))),
                getValueTypes());
    }
}
