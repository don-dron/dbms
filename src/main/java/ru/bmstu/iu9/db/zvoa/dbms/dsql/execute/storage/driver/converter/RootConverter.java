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
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.SchemeIdentification;

import java.util.Arrays;
import java.util.List;

/**
 * Root converter for schema storage reading/writting.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class RootConverter implements ByteConverter<SchemeIdentification, Schema> {
    @Override
    public @NotNull List<Type> getKeyTypes() {
        return Arrays.asList(Type.STRING);
    }

    @Override
    public @NotNull List<Type> getValueTypes() {
        return Arrays.asList(Type.STRING, Type.STRING);
    }

    @Override
    public @NotNull Schema bytesToValue(byte[] bytes, int offset) {
        List<Object> objects = BytesUtil.listFromBytes(bytes, offset, getValueTypes());
        return new DSQLSchema.Builder()
                .setSchemaName((String) objects.get(0))
                .setSchemaPath((String) objects.get(1))
                .build();
    }

    @Override
    public @NotNull SchemeIdentification bytesToKey(byte @NotNull [] bytes, int offset) {
        List<Object> objects = BytesUtil.listFromBytes(bytes, offset, getKeyTypes());
        return new SchemeIdentification((String) objects.get(0));
    }

    @Override
    public byte @NotNull [] keyToBytes(@NotNull SchemeIdentification key) {
        return BytesUtil.listObjectsToBytes(Arrays.asList(key.getName()), getKeyTypes());
    }

    @Override
    public byte @NotNull [] valueToBytes(@NotNull Schema value) {
        return BytesUtil.listObjectsToBytes(Arrays.asList(value.getSchemaName(), value.getSchemaPath()), getValueTypes());
    }
}
