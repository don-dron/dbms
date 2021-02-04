package ru.bmstu.iu9.db.zvoa.dbms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The type Dbms.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class DBMS extends AbstractDbModule {
    private final static Logger LOGGER = LoggerFactory.getLogger(DBMS.class);
    private final QueryModule queryModule;
    private final InputRequestModule inputModule;
    private final OutputResponseModule outputModule;
    private ExecutorService executorService;

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

            while (true) {
                try {
                    if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                        break;
                    } else {
                        Thread.onSpinWait();
                    }
                } catch (InterruptedException exception) {
                    LOGGER.warn(exception.getMessage());
                }
            }

            setInit();
            logInit();
        }
    }

    private void initModules() {
        initModule(queryModule.getQueryRequestStorage());
        initModule(queryModule.getQueryResponseStorage());
        initModule(queryModule);
        initModule(outputModule);
        initModule(inputModule);
    }

    private void initModule(IDbModule module) {
        executorService.execute(() -> module.init());
    }

    @Override
    public void run() {
        synchronized (this) {
            if (isRunning()) {
                return;
            }

            executorService = new ThreadPoolExecutor(
                    16,
                    Integer.MAX_VALUE,
                    Integer.MAX_VALUE,
                    TimeUnit.DAYS,
                    new SynchronousQueue<>());

            runModules();
            setRunning();
            logRunning();
            executorService.shutdown();
        }

        while (true) {
            try {
                if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                } else {
                    Thread.onSpinWait();
                }
            } catch (InterruptedException exception) {
                LOGGER.warn(exception.getMessage());
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

    /**
     * The type Builder.
     *
     * @author don-dron Zvorygin Andrey BMSTU IU-9
     */
    public static class Builder {
        private QueryModule queryModule;
        private InputRequestModule inputModule;
        private OutputResponseModule outputModule;

        /**
         * New builder builder.
         *
         * @return the builder
         */
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Sets input module.
         *
         * @param inputModule the input module
         * @return the input module
         */
        public Builder setInputModule(InputRequestModule inputModule) {
            this.inputModule = inputModule;
            return this;
        }

        /**
         * Sets output module.
         *
         * @param outputModule the output module
         * @return the output module
         */
        public Builder setOutputModule(OutputResponseModule outputModule) {
            this.outputModule = outputModule;
            return this;
        }

        /**
         * Sets query module.
         *
         * @param queryModule the query module
         * @return the query module
         */
        public Builder setQueryModule(QueryModule queryModule) {
            this.queryModule = queryModule;
            return this;
        }

        /**
         * Build dbms.
         *
         * @return the dbms
         */
        public DBMS build() {
            return new DBMS(this);
        }
    }
}
