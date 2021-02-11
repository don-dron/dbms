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
