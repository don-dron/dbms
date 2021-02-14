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
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.DBMSDataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.CreateSchemaSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.CreateTableSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.utils.DBMSUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * The type Main test.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class PipelinesTest {
    private static final String ADDRESS = "127.0.0.1";
    private static final int PORT = 8890;

    private static final int RANGE = 10000;
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
        dbms.init();
        DBMSDataStorage dataStorage = (DBMSDataStorage) dbms.getAdditionalModules().get(1);

        if (dataStorage.getSchema("schema1") == null) {
            dataStorage.createSchema(CreateSchemaSettings.Builder.newBuilder()
                    .setSchemaName("schema1")
                    .build());
            System.out.println("Create scheme");
        }

        if (dataStorage.getSchema("schema1").getTable("table1") == null) {
            dataStorage.createTable(CreateTableSettings.Builder.newBuilder()
                    .setSchemaName("schema1")
                    .setTableName("table1")
                    .setTypes(Arrays.asList(Type.INTEGER))
                    .build());
            System.out.println("Create table");
        }
    }

    /**
     * Main dummy test.
     */
    @Test
    public void mainDummyTest() {
        System.out.println("Dummy main test running");
    }

    /**
     * Insert pipeline test.
     *
     * @throws Exception the exception
     */
    @Test
    public void insertPipelineTest() throws Exception {
        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);
        CloseableHttpClient httpclient = clientBuilder.build();

        List<Thread> threads = new ArrayList<>();

        for (int i = CLIENTS_COUNT / 2; i < CLIENTS_COUNT; i++) {
            InserterThread thread = new InserterThread(httpclient, i);
            threads.add(thread);
        }

        assertTimeoutPreemptively(Duration.ofMillis(36000), () -> {
            for (Thread clientMultiThreaded : threads) {
                clientMultiThreaded.start();
            }
            for (Thread clientMultiThreaded : threads) {
                clientMultiThreaded.join();
            }
        }, () -> "Response is OK: " + counter.get() + "/" + CLIENTS_COUNT * REQUEST_PER_CLIENT);

        while (counter.get() < CLIENTS_COUNT * REQUEST_PER_CLIENT / 2) {
        }

        dbms.close();
        dbmsThread.join();
    }

    /**
     * Select pipeline test.
     *
     * @throws Exception the exception
     */
    @Test
    public void selectPipelineTest() throws Exception {
        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);
        CloseableHttpClient httpclient = clientBuilder.build();

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < CLIENTS_COUNT / 2; i++) {
            HttpPost httpPost = new HttpPost(getUri());
            String content = getSelectQuery();
            httpPost.setEntity(new StringEntity(content));

            SelectThread thread = new SelectThread(httpclient, httpPost, i);
            threads.add(thread);
        }

        assertTimeoutPreemptively(Duration.ofMillis(36000), () -> {
            for (Thread clientMultiThreaded : threads) {
                clientMultiThreaded.start();
            }
            for (Thread clientMultiThreaded : threads) {
                clientMultiThreaded.join();
            }
        }, () -> "Response is OK: " + counter.get() + "/" + CLIENTS_COUNT * REQUEST_PER_CLIENT);

        while (counter.get() < CLIENTS_COUNT * REQUEST_PER_CLIENT / 2) {
        }

        dbms.close();
        dbmsThread.join();
    }

    /**
     * Select + Insert pipeline test.
     *
     * @throws Exception the exception
     */
    @Test
    public void selectInsertPipelineTest() throws Exception {
        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);
        CloseableHttpClient httpclient = clientBuilder.build();

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < CLIENTS_COUNT / 2; i++) {
            HttpPost httpPost = new HttpPost(getUri());
            String content = getSelectQuery();
            httpPost.setEntity(new StringEntity(content));

            SelectThread thread = new SelectThread(httpclient, httpPost, i);
            threads.add(thread);
        }

        for (int i = CLIENTS_COUNT / 2; i < CLIENTS_COUNT; i++) {
            InserterThread thread = new InserterThread(httpclient, i);
            threads.add(thread);
        }

        assertTimeoutPreemptively(Duration.ofMillis(36000), () -> {
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

    /**
     * Select + Insert pipeline set test.
     *
     * @throws Exception the exception
     */
    @Test
    public void selectInsertPipelineTestWithAssertions() throws Exception {
        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);
        CloseableHttpClient httpClient = clientBuilder.build();

        Set<Integer> startValues = getAllValues(httpClient);

        for (int i = 0; i < CLIENTS_COUNT * REQUEST_PER_CLIENT; i++) {
            if (new Random().nextBoolean()) {
                int count = new Random().nextInt(RANGE);
                startValues.add(count);

                HttpPost httpPost = new HttpPost(getUri());
                String content = "" +
                        "insert into schema1.table1 values (" + count + ")\n";
                httpPost.setEntity(new StringEntity(content));
                CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpPost);
                HttpEntity entity = closeableHttpResponse.getEntity();
                EntityUtils.toString(entity);
            } else {
                Set<Integer> endValues = getAllValues(httpClient);
                assertEquals(endValues.stream().map(String::valueOf).collect(Collectors.joining(", ")),
                        startValues.stream().map(String::valueOf).collect(Collectors.joining(", ")));
            }
        }

        Set<Integer> endValues = getAllValues(httpClient);

        assertEquals(endValues.stream().map(String::valueOf).collect(Collectors.joining(", ")),
                startValues.stream().map(String::valueOf).collect(Collectors.joining(", ")));

        dbms.close();
        dbmsThread.join();
    }

    /**
     * Concurrency select + Insert pipeline set test.
     *
     * @throws Exception the exception
     */
    @Test
    public void concurrencySelectInsertPipelineTestWithAssertions() throws Exception {
        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);
        CloseableHttpClient httpClient = clientBuilder.build();

        Set<Integer> startValues = getAllValues(httpClient);
        List<Thread> threads = new ArrayList<>();
        for (int j = 0; j < CLIENTS_COUNT; j++) {
            Thread thread = new Thread(() -> {
                for (int i = 0; i < REQUEST_PER_CLIENT; i++) {
                    if (new Random().nextBoolean()) {
                        int count = new Random().nextInt(RANGE);
                        startValues.add(count);

                        HttpPost httpPost = new HttpPost(getUri());
                        String content = "" +
                                "insert into schema1.table1 values (" + count + ")\n";
                        try {
                            httpPost.setEntity(new StringEntity(content));
                            CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpPost);
                            HttpEntity entity = closeableHttpResponse.getEntity();
                            EntityUtils.toString(entity);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        try {
                            Set<Integer> endValues = getAllValues(httpClient);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            threads.add(thread);
        }

        for (int j = 0; j < CLIENTS_COUNT; j++) {
            threads.get(j).start();
        }

        for (int j = 0; j < CLIENTS_COUNT; j++) {
            threads.get(j).join();
        }

        Set<Integer> endValues = getAllValues(httpClient);

        assertEquals(endValues.stream().map(String::valueOf).collect(Collectors.joining(", ")),
                startValues.stream().map(String::valueOf).collect(Collectors.joining(", ")));

        dbms.close();
        dbmsThread.join();
    }

    /**
     * Concurrency select + Insert pipeline set test. Most write.
     *
     * @throws Exception the exception
     */
    @Test
    public void concurrencyMostWriteSelectInsertPipelineTestWithAssertions() throws Exception {
        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(100);
        HttpClientBuilder clientBuilder = HttpClients.custom().setConnectionManager(connManager);
        CloseableHttpClient httpClient = clientBuilder.build();

        Set<Integer> startValues = getAllValues(httpClient);
        List<Thread> threads = new ArrayList<>();
        for (int j = 0; j < CLIENTS_COUNT; j++) {
            Thread thread = new Thread(() -> {
                for (int i = 0; i < REQUEST_PER_CLIENT * REQUEST_PER_CLIENT; i++) {
                    if (new Random().nextInt(10) != 0) {
                        int count = new Random().nextInt(RANGE);
                        startValues.add(count);

                        HttpPost httpPost = new HttpPost(getUri());
                        String content = "" +
                                "insert into schema1.table1 values (" + count + ")\n";
                        try {
                            httpPost.setEntity(new StringEntity(content));
                            CloseableHttpResponse closeableHttpResponse = httpClient.execute(httpPost);
                            HttpEntity entity = closeableHttpResponse.getEntity();
                            EntityUtils.toString(entity);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        try {
                            Set<Integer> endValues = getAllValues(httpClient);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            threads.add(thread);
        }

        for (int j = 0; j < CLIENTS_COUNT; j++) {
            threads.get(j).start();
        }

        for (int j = 0; j < CLIENTS_COUNT; j++) {
            threads.get(j).join();
        }

        Set<Integer> endValues = getAllValues(httpClient);

        assertEquals(endValues.stream().map(String::valueOf).collect(Collectors.joining(", ")),
                startValues.stream().map(String::valueOf).collect(Collectors.joining(", ")));

        dbms.close();
        dbmsThread.join();
    }

    private Set<Integer> getAllValues(CloseableHttpClient httpClient) throws IOException {
        HttpPost scanTable = new HttpPost(getUri());
        String scanQuery = getSelectQuery();
        scanTable.setEntity(new StringEntity(scanQuery));

        CloseableHttpResponse scanResponse = httpClient.execute(scanTable);

        HttpEntity resultSet = scanResponse.getEntity();
        String values = EntityUtils.toString(resultSet);

        return values.isEmpty() ? new TreeSet<>() : Arrays.stream(values.split("\n"))
                .filter(Predicate.not(Objects::isNull))
                .map(Integer::parseInt).collect(Collectors.toCollection(ConcurrentSkipListSet::new));
    }

    private String getSelectQuery() {
        return "" +
                "select * from schema1.table1\n" +
                "where id=" + new Random().nextInt(RANGE) + "\n";
    }

    private String getUri() {
        return "http://" + ADDRESS + ":" + PORT;
    }

    public class SelectThread extends Thread {
        private final CloseableHttpClient httpClient;
        private final HttpPost httpPost;
        private final int id;

        public SelectThread(CloseableHttpClient httpClient,
                            HttpPost httpPost,
                            int id) {
            this.httpClient = httpClient;
            this.httpPost = httpPost;
            this.id = id;
        }

        @Override
        public void run() {
            for (int i = 0; i < REQUEST_PER_CLIENT; i++) {
                try {
                    CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
                    HttpEntity entity = httpResponse.getEntity();
                    if (entity != null) {
                        EntityUtils.toString(entity);
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
                    HttpPost httpPost = new HttpPost(getUri());
                    String content = getSelectQuery();
                    httpPost.setEntity(new StringEntity(content));

                    CloseableHttpResponse httpResponse = httpClient.execute(httpPost);

                    HttpEntity entity = httpResponse.getEntity();
                    if (entity != null) {
                        EntityUtils.toString(entity);
                        counter.getAndIncrement();
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }
}