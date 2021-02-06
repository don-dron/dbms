package ru.bmstu.iu9.db.zvoa.dbms;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.DSQLExecutor;
import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.DBMSServer;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.HttpRequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.HttpResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.query.DSQLQueryHandler;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * The type Main test.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class MainTest {
    private static final String ADDRESS = "127.0.0.1";
    private static final int PORT = 8890;
    private static final int CLIENTS_COUNT = 16;
    private static final int REQUEST_PER_CLIENT = 64;
    private static final Supplier<BlockingQueue> QUEUE_SUPPLIER = () -> new LinkedBlockingQueue();

    private DBMSServer httpServer;
    private DBMS dbms;
    private AtomicInteger counter;

    /**
     * Create dbms.
     */
    @BeforeEach
    public void createDBMS() {
        MockitoAnnotations.openMocks(this);

        counter = new AtomicInteger();
        httpServer = new DBMSServer(PORT);

        QueryRequestStorage queryRequestStorage =
                new QueryRequestStorage(QUEUE_SUPPLIER.get(), QUEUE_SUPPLIER.get());
        QueryResponseStorage queryResponseStorage =
                new QueryResponseStorage(QUEUE_SUPPLIER.get(), QUEUE_SUPPLIER.get());

        dbms = DBMS.Builder.newBuilder()
                .setQueryModule(QueryModule.Builder.newBuilder()
                        .setQueryHandler(new DSQLQueryHandler(new DSQLExecutor()))
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

    /**
     * Main dummy test.
     */
    @Test
    public void mainDummyTest() {
        System.out.println("Dummy main test running");
    }

    /**
     * Main pipeline test.
     *
     * @throws Exception the exception
     */
    @Test
    public void mainPipelineTest() throws Exception {
        dbms.init();
        httpServer.init();

        Thread dbmsThread = new Thread(dbms);
        Thread serverThread = new Thread(httpServer);
        dbmsThread.start();
        serverThread.start();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);
        CloseableHttpClient httpclient = clientBuilder.build();

        List<ClientMultiThreaded> threads = new ArrayList<>();
        for (int i = 0; i < CLIENTS_COUNT; i++) {
            HttpGet httpGet = new HttpGet("http://" + ADDRESS + ":" + PORT);
            ClientMultiThreaded thread = new ClientMultiThreaded(httpclient, httpGet, i);
            threads.add(thread);
        }

        assertTimeoutPreemptively(Duration.ofMillis(16000), () -> {
            for (ClientMultiThreaded clientMultiThreaded : threads) {
                clientMultiThreaded.start();
            }
            for (ClientMultiThreaded clientMultiThreaded : threads) {
                clientMultiThreaded.join();
            }
        }, () -> "Response is OK: " + counter.get() + "/" + CLIENTS_COUNT * REQUEST_PER_CLIENT);

        httpServer.close();
        dbms.close();
        dbmsThread.join();
        serverThread.join();
    }

    /**
     * The type Client multi threaded.
     *
     * @author don-dron Zvorygin Andrey BMSTU IU-9
     */
    public class ClientMultiThreaded extends Thread {
        private final CloseableHttpClient httpClient;
        private final HttpGet httpget;
        private final int id;

        /**
         * Instantiates a new Client multi threaded.
         *
         * @param httpClient the http client
         * @param httpget    the httpget
         * @param id         the id
         */
        public ClientMultiThreaded(CloseableHttpClient httpClient,
                                   HttpGet httpget,
                                   int id) {
            this.httpClient = httpClient;
            this.httpget = httpget;
            this.id = id;
        }

        @Override
        public void run() {
            for (int i = 0; i < REQUEST_PER_CLIENT; i++) {
                try {
                    CloseableHttpResponse httpResponse = httpClient.execute(httpget);

                    System.out.println("Status of thread " + id + ":" + httpResponse.getStatusLine());

                    HttpEntity entity = httpResponse.getEntity();
                    if (entity != null) {
                        System.out.println("Bytes read by thread thread " + id + " : " + EntityUtils.toString(entity));
                        counter.getAndIncrement();
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }
}
