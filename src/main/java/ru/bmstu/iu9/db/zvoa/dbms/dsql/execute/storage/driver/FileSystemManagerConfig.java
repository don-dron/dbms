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

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;

import java.util.function.Function;

public class FileSystemManagerConfig {
    private final StorageProperties storageProperties;
    private final Function<StorageProperties, IKeyValueStorage> storageSupplier;

    private FileSystemManagerConfig(Builder builder) {
        this.storageProperties = builder.storageProperties;
        this.storageSupplier = builder.storageSupplier;
    }

    public Function<StorageProperties, IKeyValueStorage> getStorageSupplier() {
        return storageSupplier;
    }

    public StorageProperties getStorageProperties() {
        return storageProperties;
    }

    public static class Builder {
        private StorageProperties storageProperties;
        private Function<StorageProperties, IKeyValueStorage> storageSupplier;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setStorageProperties(StorageProperties storageProperties) {
            this.storageProperties = storageProperties;
            return this;
        }

        public Builder setStorageSupplier(Function<StorageProperties, IKeyValueStorage> storageSupplier) {
            this.storageSupplier = storageSupplier;
            return this;
        }

        public FileSystemManagerConfig build() {
            return new FileSystemManagerConfig(this);
        }
    }
}
