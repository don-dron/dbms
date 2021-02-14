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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The Data Base storage - it's two sides buffer between modules for sharing data.
 *
 * @param <T> the type elements in storage
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class DbStorage<T> extends AbstractDbModule implements IDbStorage<T> {
    private final Logger logger = LoggerFactory.getLogger(DbStorage.class);

    private final BlockingQueue<T> inputBuffer;
    private final BlockingQueue<T> outputBuffer;

    public DbStorage(BlockingQueue<T> inputBuffer,
                     BlockingQueue<T> outputBuffer) {
        this.inputBuffer = inputBuffer;
        this.outputBuffer = outputBuffer;
    }

    @Override
    public boolean isEmpty() {
        return outputBuffer.isEmpty();
    }

    @Override
    public boolean put(T t) {
        try {
            logger.debug("Put " + t + " to storage " + getClass().getSimpleName()
                    + " input buffer size " + inputBuffer.size()
                    + " output buffer size " + outputBuffer.size());
            inputBuffer.put(t);
            return true;
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
            return false;
        }
    }

    @Override
    public T get() {
        try {
            T t = outputBuffer.poll(1, TimeUnit.MILLISECONDS);
            if (t != null) {
                logger.debug("Get " + t + " from storage " + getClass().getSimpleName()
                        + " input buffer size " + inputBuffer.size()
                        + " output buffer size " + outputBuffer.size());
                return t;
            }
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
        return null;
    }

    @Override
    public void init() {
        synchronized (this) {
            if (isInit()) {
                return;
            }
            logInit();
            setInit();
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
            T t;
            try {
                if (!inputBuffer.isEmpty() &&
                        (t = inputBuffer.poll(1, TimeUnit.MILLISECONDS)) != null) {
                    try {
                        outputBuffer.put(t);
                        logger.debug("Replace " + t + getClass().getSimpleName());
                    } catch (InterruptedException e) {
                        logger.warn(e.getMessage());
                    }
                } else {
                    Thread.yield();
                }
            } catch (InterruptedException e) {
                logger.warn(e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (isClosed()) {
                return;
            }

            if (inputBuffer.isEmpty() && outputBuffer.isEmpty()) {
                setClosed();
                logClose();
            } else {
                throw new StorageException("Non empty storage.");
            }
        }
    }
}
