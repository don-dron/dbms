package ru.bmstu.iu9.db.zvoa.dbms.core.run.main;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    public void mainPipelineTest() {
        try {
            dbms.init();
            dbms.run();

            Thread serverThread = new Thread(httpServer::run);
            serverThread.start();

            HttpClient httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_2)
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .setHeader("TTTT", "111")
                    .uri(URI.create("http://" + ADDRESS + ":" + PORT))
                    .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            response.body();

            serverThread.join();
        } catch (Exception exception) {
            System.out.println(exception.getMessage());
        }
    }
}
