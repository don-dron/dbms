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
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;

import java.util.function.Supplier;

/**
 * The type Input request module.
 *
 * @param <T> the type parameter
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class InputRequestModule<T> extends AbstractDbModule {
    private final Logger logger = LoggerFactory.getLogger(InputRequestModule.class);
    private final Supplier<T> queryGenerator;
    private final QueryRequestStorage queryRequestStorage;
    private final RequestHandler<T> requestHandler;

    /**
     * Instantiates a new Input request module.
     *
     * @param requestGenerator    the request generator
     * @param queryRequestStorage the query request storage
     * @param requestHandler      the request handler
     */
    public InputRequestModule(
            Supplier<T> requestGenerator,
            QueryRequestStorage queryRequestStorage,
            RequestHandler<T> requestHandler) {
        this.queryGenerator = requestGenerator;
        this.queryRequestStorage = queryRequestStorage;
        this.requestHandler = requestHandler;
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

        while (isRunning()) {
            T t = queryGenerator.get();

            if (t != null) {
                logger.debug("Input module handle request " + t);
                queryRequestStorage.put(requestHandler.execute(t));
            } else {
                Thread.onSpinWait();
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
     * Gets query request storage.
     *
     * @return the query request storage
     */
    public QueryRequestStorage getQueryRequestStorage() {
        return queryRequestStorage;
    }
}
