package ru.bmstu.iu9.db.zvoa.dbms;

import java.util.concurrent.*;

import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;

public class DBMS extends AbstractDbModule {

    private ExecutorService executorService;

    private QueryModule queryModule;
    private InputRequestModule inputModule;
    private OutputResponseModule outputModule;

    private DBMS(Builder builder) {
        this.queryModule = builder.queryModule;
        this.inputModule = builder.inputModule;
        this.outputModule = builder.outputModule;
    }

    @Override
    public synchronized void init() {
        synchronized (this) {
            if (isInit()) {
                return;
            }
            executorService = new ThreadPoolExecutor(16, Integer.MAX_VALUE,
                    600L, TimeUnit.DAYS,
                    new SynchronousQueue<Runnable>());
            initModules();

            executorService.shutdown();
        }
        while (true) {
            try {
                if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
            }
        }
    }

    private void initModules() {
        initModule(queryModule.getQueryRequestStorage());
        initModule(queryModule.getQueryResponseStorage());
        initModule(queryModule);
        initModule(outputModule);
        initModule(inputModule);
        setInit();
        logInit();
    }

    private void initModule(IDbModule module) {
        executorService.execute(() -> module.init());
    }

    public void run() {
        synchronized (this) {
            if (isRunning()) {
                return;
            }

            executorService = new ThreadPoolExecutor(16, Integer.MAX_VALUE,
                    600L, TimeUnit.DAYS,
                    new SynchronousQueue<Runnable>());

            runModules();
            setRunning();
            logRunning();
            executorService.shutdown();
        }
        while (true) {
            try {
                if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
            }
        }
    }

    private void runModules() {
        runModule(queryModule.getQueryRequestStorage());
        runModule(queryModule.getQueryResponseStorage());
        runModule(queryModule);
        runModule(inputModule);
        runModule(outputModule);
    }

    private void runModule(IDbModule dbModule) {
        executorService.execute(dbModule);
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            queryModule.close();
            queryModule.getQueryRequestStorage().close();
            queryModule.getQueryResponseStorage().close();
            inputModule.close();
            outputModule.close();
            setClosed();
            logClose();
        }
    }

    public static class Builder {
        private QueryModule queryModule;
        private InputRequestModule inputModule;
        private OutputResponseModule outputModule;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setInputModule(InputRequestModule inputModule) {
            this.inputModule = inputModule;
            return this;
        }

        public Builder setOutputModule(OutputResponseModule outputModule) {
            this.outputModule = outputModule;
            return this;
        }

        public Builder setQueryModule(QueryModule queryModule) {
            this.queryModule = queryModule;
            return this;
        }

        public DBMS build() {
            return new DBMS(this);
        }
    }
}
