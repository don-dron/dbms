package ru.bmstu.iu9.db.zvoa.dbms.main;

import ru.bmstu.iu9.db.zvoa.dbms.DBMS;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.DSQLExecutor;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.JSQLInterpreter;
import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.DBMSServer;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.HttpRequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.HttpResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.query.DSQLQueryHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * The type Main.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class Main {
    private static final Supplier<BlockingQueue> queueSupplier = () -> new LinkedBlockingQueue();
    private static final String ADDRESS = "127.0.0.1";
    private static final int PORT = 38900;

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        DBMSServer httpServer = new DBMSServer(PORT);

        QueryRequestStorage queryRequestStorage =
                new QueryRequestStorage(queueSupplier.get(), queueSupplier.get());
        QueryResponseStorage queryResponseStorage =
                new QueryResponseStorage(queueSupplier.get(), queueSupplier.get());

        DBMS dbms = DBMS.Builder.newBuilder()
                .setQueryModule(QueryModule.Builder.newBuilder()
                        .setQueryHandler(new DSQLQueryHandler(new JSQLInterpreter()))
                        .setQueryRequestStorage(queryRequestStorage)
                        .setQueryResponseStorage(queryResponseStorage)
                        .build())
                .setInputModule(new InputRequestModule(httpServer,
                        queryRequestStorage,
                        new HttpRequestHandler()))
                .setOutputModule(new OutputResponseModule(httpServer,
                        queryResponseStorage,
                        new HttpResponseHandler()))
                .build();

        dbms.init();
        httpServer.init();

        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();
        Thread serverThread = new Thread(httpServer);
        serverThread.start();

        dbmsThread.join();
        serverThread.join();
    }
}
