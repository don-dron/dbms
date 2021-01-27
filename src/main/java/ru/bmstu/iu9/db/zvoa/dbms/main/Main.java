package ru.bmstu.iu9.db.zvoa.dbms.main;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import ru.bmstu.iu9.db.zvoa.dbms.DBMS;
import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.impl.HttpRequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.io.impl.HttpResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.io.impl.HttpServer;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

public class Main {
    private static Supplier<BlockingQueue> queueSupplier = () -> new LinkedBlockingQueue();

    public static void main(String[] args) {
        QueryRequestStorage queryRequestStorage =
                new QueryRequestStorage(queueSupplier.get(), queueSupplier.get());
        QueryResponseStorage queryResponseStorage =
                new QueryResponseStorage(queueSupplier.get(), queueSupplier.get());

        try (HttpServer httpServer = new HttpServer(); DBMS dbms = DBMS.Builder.newBuilder()
                .setInputModule(null)
                .setOutputModule(null)
                .setQueryModule(QueryModule.Builder.newBuilder()
                        .setQueryHandler(new QueryHandler())
                        .setQueryRequestStorage(queryRequestStorage)
                        .setQueryResponseStorage(queryResponseStorage)
                        .build())
                .setInputModule(new InputRequestModule(httpServer,
                        queryRequestStorage,
                        new HttpRequestHandler()))
                .setOutputModule(new OutputResponseModule(httpServer,
                        queryResponseStorage,
                        new HttpResponseHandler()))
                .build()) {
            dbms.init();
            dbms.run();

            httpServer.init();
            httpServer.run();
        } catch (Exception exception) {
        }
    }
}
