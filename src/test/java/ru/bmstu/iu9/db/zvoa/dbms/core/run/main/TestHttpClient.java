package ru.bmstu.iu9.db.zvoa.dbms.core.run.main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHttpClient {
    private final String uri;
    private int count;

    public TestHttpClient(String uri, int count) {
        this.uri = uri;
        this.count = count;
    }

    public void run() {
        try {
            HttpClient client = HttpClient.newHttpClient();

            while (count-- > 0) {
                HttpRequest request = HttpRequest.newBuilder(new URI(uri)).build();
                HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                assertEquals(response.statusCode(), 200);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
