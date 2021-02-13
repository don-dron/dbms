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
