package ru.bmstu.iu9.db.zvoa.dbms;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    public void init() {
        executorService = Executors.newCachedThreadPool();
        initModules();
        executorService.shutdown();
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
        executorService.submit(() -> module.init());
    }

    public void run() {
        executorService = Executors.newCachedThreadPool();
        runModules();
        setRunning();
        logRunning();
        executorService.shutdown();
    }

    private void runModules() {
        runModule(queryModule.getQueryRequestStorage());
        runModule(queryModule.getQueryResponseStorage());
        runModule(queryModule);
        runModule(outputModule);
        runModule(inputModule);
    }

    private void runModule(IDbModule dbModule) {
        executorService.submit(dbModule);
    }

    @Override
    public void close() throws Exception {
        setRunning();
        logRunning();
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
