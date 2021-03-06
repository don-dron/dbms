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
package ru.bmstu.iu9.db.zvoa.dbms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * The type Dbms.
 *
 * @author don -dron Zvorygin Andrey BMSTU IU-9
 */
public class DBMS extends AbstractDbModule {
    private final Logger logger = LoggerFactory.getLogger(DBMS.class);
    private final List<QueryModule> queryModules;
    private final InputRequestModule inputModule;
    private final OutputResponseModule outputModule;
    private final List<IDbModule> additionalModules;

    private DBMS(Builder builder) {
        this.inputModule = builder.inputModule;
        this.queryModules = new ArrayList<>();

        for (int i = 0; i < 8; i++) {
            queryModules.add(builder.queryModule.build());
        }

        this.outputModule = builder.outputModule;
        this.additionalModules = builder.additionalModules;
    }

    @Override
    public synchronized void init() {
        if (isInit()) {
            return;
        }

        ExecutorService executorService = createExecutorService();
        initModules(executorService);
        executorService.shutdown();

        joinExecutorService(executorService);

        setInit();
        logInit();
    }

    @Override
    public void run() {
        ExecutorService executorService;
        synchronized (this) {
            if (isRunning()) {
                return;
            }

            executorService = createExecutorService();

            runModules(executorService);
            setRunning();
            logRunning();
        }

        executorService.shutdown();
        joinExecutorService(executorService);
    }

    public List<IDbModule> getAdditionalModules() {
        return additionalModules;
    }

    private void joinExecutorService(ExecutorService executorService) {
        while (true) {
            try {
                if (executorService.awaitTermination(0, TimeUnit.MILLISECONDS)) {
                    break;
                } else {
                    Thread.yield();
                }
            } catch (InterruptedException exception) {
                logger.warn(exception.getMessage());
            }
        }
    }

    private ExecutorService createExecutorService() {
        return new ScheduledThreadPoolExecutor(
                16);
    }

    private void initModules(ExecutorService executorService) {
        for (IDbModule module : additionalModules) {
            initModule(executorService, module);
        }

        initModule(executorService, inputModule.getQueryRequestStorage());
        initModule(executorService, outputModule.getQueryResponseStorage());

        for (QueryModule queryModule : queryModules) {
            initModule(executorService, queryModule);
        }

        initModule(executorService, outputModule);
        initModule(executorService, inputModule);
    }

    private void runModules(ExecutorService executorService) {
        runModule(executorService, inputModule.getQueryRequestStorage());
        runModule(executorService, outputModule.getQueryResponseStorage());

        for (QueryModule queryModule : queryModules) {
            runModule(executorService, queryModule);
        }

        runModule(executorService, inputModule);
        runModule(executorService, outputModule);

        for (IDbModule module : additionalModules) {
            runModule(executorService, module);
        }
    }

    private void initModule(ExecutorService executorService, IDbModule module) {
        executorService.execute(
                () -> {
                    try {
                        module.init();
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                });
    }

    private void runModule(ExecutorService executorService, IDbModule dbModule) {
        executorService.execute(dbModule);
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }

            inputModule.close();
            outputModule.close();

            for (QueryModule queryModule : queryModules) {
                queryModule.close();
            }

            inputModule.getQueryRequestStorage().close();
            outputModule.getQueryResponseStorage().close();

            for (IDbModule module : additionalModules) {
                module.close();
            }

            setClosed();
            logClose();
        }
    }

    /**
     * The type Builder.
     *
     * @author don -dron Zvorygin Andrey BMSTU IU-9
     */
    public static class Builder {
        private QueryModule.Builder queryModule;
        private InputRequestModule inputModule;
        private OutputResponseModule outputModule;
        private List<IDbModule> additionalModules = Collections.emptyList();

        /**
         * New builder builder.
         *
         * @return the builder
         */
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Sets additional modules.
         *
         * @param additionalModules the additional modules
         * @return the additional modules
         */
        public Builder setAdditionalModules(List<IDbModule> additionalModules) {
            assert (additionalModules != null);
            this.additionalModules = additionalModules;
            return this;
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
         * @return the query module
         */
        public Builder setQueryModuleBuilder(QueryModule.Builder queryModuleBuilder) {
            this.queryModule = queryModuleBuilder;
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
