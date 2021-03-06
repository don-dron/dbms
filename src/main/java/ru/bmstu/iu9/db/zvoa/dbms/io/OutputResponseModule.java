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
package ru.bmstu.iu9.db.zvoa.dbms.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

import java.util.function.Consumer;

/**
 * The type Output response module.
 *
 * @param <T> the type parameter
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class OutputResponseModule<T> extends AbstractDbModule {
    private final Logger logger = LoggerFactory.getLogger(OutputResponseModule.class);

    private final QueryResponseStorage queryResponseStorage;
    private final ResponseHandler<T> responseHandler;
    private final Consumer<T> consumer;

    /**
     * Instantiates a new Output response module.
     *
     * @param consumer             the consumer
     * @param queryResponseStorage the query response storage
     * @param responseHandler      the response handler
     */
    public OutputResponseModule(
            Consumer<T> consumer,
            QueryResponseStorage queryResponseStorage,
            ResponseHandler<T> responseHandler) {
        this.consumer = consumer;
        this.queryResponseStorage = queryResponseStorage;
        this.responseHandler = responseHandler;
    }

    @Override
    public void init() {
        synchronized (this) {
            if (isInit()) {
                return;
            }
            setInit();
            logInit();
        }
    }

    @Override
    public void run() {
        synchronized (this) {
            if (isRunning()) {
                return;
            }
            if (isClosed()) {
                return;
            }
            setRunning();
            logRunning();
        }

        Query response;
        while (isRunning()) {
            if (!queryResponseStorage.isEmpty() &&
                    (response = queryResponseStorage.get()) != null) {
                T t = responseHandler.execute(response);
                logger.debug("Output module handle response " + t);
                consumer.accept(t);
            } else {
                Thread.yield();
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            setClosed();
            logClose();
        }
    }

    /**
     * Gets query response storage.
     *
     * @return the query response storage
     */
    public QueryResponseStorage getQueryResponseStorage() {
        return queryResponseStorage;
    }
}
