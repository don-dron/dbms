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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.ByteConverter;

public class StorageProperties<K extends Key, V extends Value> {
    private String path;
    private String name;
    private ByteConverter<K, V> byteConverter;

    public StorageProperties(ByteConverter<K, V> byteConverter, String name, String path) {
        this.name = name;
        this.path = path;
        this.byteConverter = byteConverter;
    }

    public String getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public ByteConverter<K, V> getByteConverter() {
        return byteConverter;
    }

    public enum StorageType {
        ROOT,
        SCHEMA,
        TABLE
    }
}
