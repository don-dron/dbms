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
    public boolean put(T t) {
        try {
            inputBuffer.put(t);
//            logger.info("Put " + t + " to storage " + getClass().getSimpleName());
            return true;
        } catch (InterruptedException e) {
//            logger.info("Put failed" + t + " to storage " + getClass().getSimpleName());
            return false;
        }
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
            try {
                outputBuffer.put(inputBuffer.poll(10, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (isClosed()) {
            return;
        }
        setClosed();
        logClose();
    }

    @Override
    public T get() {
        try {
            T t = outputBuffer.poll(10, TimeUnit.MILLISECONDS);
            if (t != null) {
//                logger.info("Get " + t + " from storage " + getClass().getSimpleName());
                return t;
            }
        } catch (InterruptedException e) {
        }

//        logger.info("Cannot get item from storage " + getClass().getSimpleName());
        return null;
    }
}
