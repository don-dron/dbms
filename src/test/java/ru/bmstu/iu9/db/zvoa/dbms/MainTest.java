package ru.bmstu.iu9.db.zvoa.dbms;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.JSQLInterpreter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.DBMSDataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.DBMSServer;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.HttpRequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.HttpResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.query.DSQLQueryHandler;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

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
    private static final int REQUEST_PER_CLIENT = 16;
    private static final String SCHEMA_NAME = "schema1";
    private static final String TABLE_NAME = "table1";
    private static final Supplier<BlockingQueue> QUEUE_SUPPLIER = () -> new LinkedBlockingQueue();

    private DBMSServer httpServer;
    private DBMS dbms;
    private AtomicInteger counter;

    /**
     * Create dbms.
     */
    @BeforeEach
    public void createDBMS() throws DataStorageException {
        MockitoAnnotations.openMocks(this);

        counter = new AtomicInteger();
        httpServer = new DBMSServer(PORT);
        // TODO сделать нормально
        File file = new File("./");
        String path = file.getAbsolutePath();
        path = path.substring(0, path.length() - 1) + "data";

        DataStorage dataStorage = new DBMSDataStorage.Builder()
                .setDirectory(path).build();


        dataStorage.createSchema(CreateSchemaSettings.Builder.newBuilder()
                .setSchemaName(SCHEMA_NAME)
                .build());

        dataStorage.createTable(CreateTableSettings.Builder.newBuilder()
                .setSchemaName(SCHEMA_NAME)
                .setTableName(TABLE_NAME)
                .setTypes(Arrays.asList(Type.INTEGER))
                .build());

        QueryRequestStorage queryRequestStorage =
                new QueryRequestStorage(QUEUE_SUPPLIER.get(), QUEUE_SUPPLIER.get());
        QueryResponseStorage queryResponseStorage =
                new QueryResponseStorage(QUEUE_SUPPLIER.get(), QUEUE_SUPPLIER.get());

        dbms = DBMS.Builder.newBuilder()
                .setQueryModule(QueryModule.Builder.newBuilder()
                        .setQueryHandler(new DSQLQueryHandler(new JSQLInterpreter(dataStorage)))
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

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < CLIENTS_COUNT / 2; i++) {
            HttpPost httpPost = new HttpPost("http://" + ADDRESS + ":" + PORT);
            String content = "" +
                    "\n" +
                    "select * from schema1.table1\n";
            httpPost.setEntity(new StringEntity(content));

            ClientMultiThreaded thread = new ClientMultiThreaded(httpclient, httpPost, i);
            threads.add(thread);
        }

        for (int i = CLIENTS_COUNT / 2; i < CLIENTS_COUNT; i++) {
            ClientMultiThreaded1 thread = new ClientMultiThreaded1(httpclient, i);
            threads.add(thread);
        }

        assertTimeoutPreemptively(Duration.ofMillis(24000), () -> {
            for (Thread clientMultiThreaded : threads) {
                clientMultiThreaded.start();
            }
            for (Thread clientMultiThreaded : threads) {
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
        private final HttpPost httpget;
        private final int id;

        /**
         * Instantiates a new Client multi threaded.
         *
         * @param httpClient the http client
         * @param httpGet    the httpGet
         * @param id         the id
         */
        public ClientMultiThreaded(CloseableHttpClient httpClient,
                                   HttpPost httpGet,
                                   int id) {
            this.httpClient = httpClient;
            this.httpget = httpGet;
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


    /**
     * The type Client multi threaded.
     *
     * @author don-dron Zvorygin Andrey BMSTU IU-9
     */
    public class ClientMultiThreaded1 extends Thread {
        private final CloseableHttpClient httpClient;
        private final int id;

        /**
         * Instantiates a new Client multi threaded.
         *
         * @param httpClient the http client
         * @param httpGet    the httpGet
         * @param id         the id
         */
        public ClientMultiThreaded1(CloseableHttpClient httpClient,
                                    int id) {
            this.httpClient = httpClient;
            this.id = id;
        }

        @Override
        public void run() {
            for (int i = 0; i < REQUEST_PER_CLIENT * REQUEST_PER_CLIENT; i++) {
                try {
                    HttpPost httpPost = new HttpPost("http://" + ADDRESS + ":" + PORT);
                    String content = "" +
                            "\n" +
                            "insert into schema1.table1 values (" + new Random().nextInt(1000000) + ")\n";
                    httpPost.setEntity(new StringEntity(content));

                    CloseableHttpResponse httpResponse = httpClient.execute(httpPost);

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