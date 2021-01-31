package ru.bmstu.iu9.db.zvoa.dbms.modules;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class DbStorage<T> extends AbstractDbModule implements IDbStorage<T> {

    private final BlockingQueue<T> inputBuffer;
    private final BlockingQueue<T> outputBuffer;
    private final Logger logger;

    public DbStorage(BlockingQueue inputBuffer,
                     BlockingQueue outputBuffer) {
        this.inputBuffer = inputBuffer;
        this.outputBuffer = outputBuffer;
        this.logger = getLogger();
    }

    @Override
    public boolean isEmpty() {
        return outputBuffer.isEmpty();
    }

    @Override
    public boolean put(T t) {
        try {
            logger.info("Put " + t + " to storage " + getClass().getSimpleName()
                    + " input buffer size " + inputBuffer.size()
                    + " output buffer size " + outputBuffer.size());
            inputBuffer.put(t);
            inputBuffer.notifyAll();
            return true;
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
            return false;
        }
    }

    @Override
    public T get() {
        try {
            T t = outputBuffer.poll(1, TimeUnit.MILLISECONDS);
            if (t != null) {
                logger.info("Get " + t + " from storage " + getClass().getSimpleName()
                        + " input buffer size " + inputBuffer.size()
                        + " output buffer size " + outputBuffer.size());
//                notifyAll();
                return t;
            }
        } catch (InterruptedException e) {
            logger.warning(e.getMessage());
        }
        return null;
    }

    @Override
    public synchronized void init() {
        if (isInit()) {
            return;
        }
        logInit();
        setInit();
    }

    @Override
    public synchronized void run() {
        if (isRunning()) {
            return;
        }
        setRunning();
        logRunning();
        while (isRunning()) {
//            System.out.println(getClass().getSimpleName());
            T t;
            try {
                if (!inputBuffer.isEmpty() &&
                        (t = inputBuffer.poll(1, TimeUnit.MILLISECONDS)) != null) {
                    try {
                        outputBuffer.put(t);
                        notifyAll();
                        logger.info("Replace " + t + getClass().getSimpleName());
                    } catch (InterruptedException e) {
                        logger.warning(e.getMessage());
                    }
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//                    try {
//                        inputBuffer.wait();
//                    } catch (InterruptedException exception) {
//
//                    }
                }
            } catch (InterruptedException e) {
                logger.warning(e.getMessage());
            }
        }
    }

    @Override
    public synchronized void close() throws Exception {
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
