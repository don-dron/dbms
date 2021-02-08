package ru.bmstu.iu9.db.zvoa.dbms.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public class DBMSConfig {
    private final String mountPath;
    private final int port;
    private final Supplier<BlockingQueue> queueSupplier;

    private DBMSConfig(Builder builder) {
        mountPath = builder.mountPath;
        port = builder.port;
        queueSupplier = builder.queueSupplier;
    }

    public Supplier<BlockingQueue> getQueueSupplier() {
        return queueSupplier;
    }

    public int getPort() {
        return port;
    }

    public String getMountPath() {
        return mountPath;
    }

    public static class Builder {
        private String mountPath;
        private int port;
        private Supplier<BlockingQueue> queueSupplier = () -> new LinkedBlockingQueue();

        public Builder setMountPath(String mountPath) {
            this.mountPath = mountPath;
            return this;
        }
            
        public Builder setQueueSupplier(Supplier<BlockingQueue> queueSupplier) {
            this.queueSupplier = queueSupplier;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public DBMSConfig build() {
            return new DBMSConfig(this);
        }
    }
}
