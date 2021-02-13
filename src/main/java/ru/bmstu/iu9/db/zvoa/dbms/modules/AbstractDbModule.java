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
package ru.bmstu.iu9.db.zvoa.dbms.modules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

import java.io.IOException;

/**
 * The type Abstract db module.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public abstract class AbstractDbModule implements IDbModule {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private volatile State state = State.NOT_INIT;

    @Override
    public abstract void init() throws DataStorageException, IOException;

    @Override
    public abstract void run();

    @Override
    public abstract void close() throws Exception;

    @Override
    public boolean isInit() {
        return state != State.NOT_INIT;
    }

    @Override
    public boolean isRunning() {
        return state == State.RUNNING;
    }

    @Override
    public boolean isClosed() {
        return state == State.CLOSED;
    }

    /**
     * Sets init.
     */
    protected void setInit() {
        state = State.INIT;
    }

    /**
     * Sets running.
     */
    protected void setRunning() {
        state = State.RUNNING;
    }

    /**
     * Sets closed.
     */
    protected void setClosed() {
        state = State.CLOSED;
    }

    /**
     * Log init.
     */
    protected void logInit() {
        logger.debug("Init " + getClass().getSimpleName() + " module.");
    }

    /**
     * Log running.
     */
    protected void logRunning() {
        logger.debug("Running " + getClass().getSimpleName() + " module.");
    }

    /**
     * Log close.
     */
    protected void logClose() {
        logger.debug("Close " + getClass().getSimpleName() + " module.");
    }

    private enum State {
        NOT_INIT,
        INIT,
        RUNNING,
        CLOSED
    }
}
