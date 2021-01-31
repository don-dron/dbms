package ru.bmstu.iu9.db.zvoa.dbms.core.run.main;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import ru.bmstu.iu9.db.zvoa.dbms.DBMS;
import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.impl.DBMSServer;
import ru.bmstu.iu9.db.zvoa.dbms.io.impl.HttpRequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.io.impl.HttpResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

import static org.junit.jupiter.api.Assertions.*;

public class MainTest {
    private static final String ADDRESS = "127.0.0.1";
    private static final int PORT = 8890;
    private static Supplier<BlockingQueue> queueSupplier = () -> new LinkedBlockingQueue();
    private DBMSServer httpServer;
    private DBMS dbms;

    @BeforeEach
    public void createDBMS() {
        MockitoAnnotations.openMocks(this);

        httpServer = new DBMSServer(PORT);

        QueryRequestStorage queryRequestStorage =
                new QueryRequestStorage(queueSupplier.get(), queueSupplier.get());
        QueryResponseStorage queryResponseStorage =
                new QueryResponseStorage(queueSupplier.get(), queueSupplier.get());

        dbms = DBMS.Builder.newBuilder()
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
                .build();
    }

    @Test
    public void mainDummyTest() {
        System.out.println("Dummy main test running");
    }

    @Test
    public void mainPipelineTest() throws Exception {
        dbms.init();
        httpServer.init();

        Thread dbmsThread = new Thread(dbms::run);
        dbmsThread.start();

        Thread serverThread = new Thread(httpServer::run);
        serverThread.start();

        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        ExecutorService joinPool = new ThreadPoolExecutor(16, Integer.MAX_VALUE,
                600L, TimeUnit.DAYS,
                new SynchronousQueue<Runnable>());

        for (int i = 0; i < 10; i++) {
            joinPool.execute(() -> {
                assertTimeout(Duration.ofMillis(4000), () -> {
                    for (int j = 0; j < 30; j++) {
                        HttpRequest request = HttpRequest.newBuilder()
                                .version(HttpClient.Version.HTTP_1_1)
                                .uri(URI.create("http://" + ADDRESS + ":" + PORT))
                                .build();

                        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                        response.body();
                    }
                });
            });
        }

        joinPool.shutdown();
        while (true) {
            try {
                if (joinPool.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
            }
        }

        httpServer.close();
        dbms.close();
        dbmsThread.join();
        serverThread.join();
    }
}
