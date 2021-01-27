package ru.bmstu.iu9.db.zvoa.dbms;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbModule;

public class DBMSJob implements Runnable {
    public IDbModule module;

    public DBMSJob(IDbModule module) {
        this.module = module;
    }

    @Override
    public void run() {
        try (IDbModule dbModule = module) {
            dbModule.run();
        } catch (Exception exception) {
        }
    }
}
