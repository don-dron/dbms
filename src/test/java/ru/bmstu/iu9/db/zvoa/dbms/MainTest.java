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
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.utils.DBMSUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * The type Main test.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class MainTest {
    private static final String ADDRESS = "127.0.0.1";
    private static final int PORT = 8890;

    private static final int RANGE = 1000000;
    private static final int CLIENTS_COUNT = 16;
    private static final int REQUEST_PER_CLIENT = 16;
    private static final String SCHEMA_NAME = "schema1";
    private static final String TABLE_NAME = "table1";

    private DBMS dbms;
    private AtomicInteger counter;

    /**
     * Create dbms.
     */
    @BeforeEach
    public void createDBMS() throws DataStorageException, IOException {
        MockitoAnnotations.openMocks(this);
        counter = new AtomicInteger();
        dbms = DBMSUtils.createDefaultDBMS();
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
        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);
        CloseableHttpClient httpclient = clientBuilder.build();

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < CLIENTS_COUNT / 2; i++) {
            HttpPost httpPost = new HttpPost("http://" + ADDRESS + ":" + PORT);
            String content = "" +
                    "\n" +
                    "select * from schema1.table1\n" +
                    "where id=" + new Random().nextInt(RANGE) + "\n";
            httpPost.setEntity(new StringEntity(content));

            SelectThread thread = new SelectThread(httpclient, httpPost, i);
            threads.add(thread);
        }

        for (int i = CLIENTS_COUNT / 2; i < CLIENTS_COUNT; i++) {
            InserterThread thread = new InserterThread(httpclient, i);
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

        while (counter.get() < CLIENTS_COUNT * REQUEST_PER_CLIENT) {
        }

        dbms.close();
        dbmsThread.join();
    }

    public class SelectThread extends Thread {
        private final CloseableHttpClient httpClient;
        private final HttpPost httpGet;
        private final int id;

        public SelectThread(CloseableHttpClient httpClient,
                            HttpPost httpGet,
                            int id) {
            this.httpClient = httpClient;
            this.httpGet = httpGet;
            this.id = id;
        }

        @Override
        public void run() {
            for (int i = 0; i < REQUEST_PER_CLIENT; i++) {
                try {
                    CloseableHttpResponse httpResponse = httpClient.execute(httpGet);

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

    public class InserterThread extends Thread {
        private final CloseableHttpClient httpClient;
        private final int id;

        public InserterThread(CloseableHttpClient httpClient,
                              int id) {
            this.httpClient = httpClient;
            this.id = id;
        }

        @Override
        public void run() {
            for (int i = 0; i < REQUEST_PER_CLIENT; i++) {
                try {
                    HttpPost httpPost = new HttpPost("http://" + ADDRESS + ":" + PORT);
                    String content = "" +
                            "\n" +
                            "insert into schema1.table1 values (" + new Random().nextInt(RANGE) + ")\n";
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