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
                    Thread.onSpinWait();
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
