package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

public class HttpServer extends AbstractDbModule implements Supplier<String>, Consumer<String> {

    private final List<SocketHandler> handlers = new ArrayList<>();
    private final BlockingQueue<String> inputBuffer = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> outputBuffer = new LinkedBlockingQueue<>();
    private ThreadPoolExecutor threadPoolExecutor;
    private ServerSocket server;

    @Override
    public String get() {
        try {
            return inputBuffer.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public void accept(String httpResponse) {
        try {
            outputBuffer.put(httpResponse);
        } catch (InterruptedException e) {
            //
        }
    }


    @Override
    public void init() {
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        try {
            server = new ServerSocket(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        setInit();
        logInit();
    }

    @Override
    public void run() {
        setRunning();
        logRunning();
        try {
            while (true) {
                try (Socket socket = server.accept()) {
                    handlers.add(new SocketHandler(socket));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } finally {
        }
    }

    @Override
    public void close() throws Exception {
        server.close();
        setClosed();
        logClose();
    }

    private class SocketHandler implements Runnable {
        private Socket socket;
        private BufferedReader in;
        private BufferedWriter out;

        public SocketHandler(Socket socket) throws IOException {
            this.socket = socket;
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        }

        @Override
        public void run() {
            try {
                while (!socket.isClosed()) {
                    StringBuilder buffer = new StringBuilder();
                    String line;
                    while ((line = in.readLine()) != null) {
                        buffer.append(line);
                    }
                    inputBuffer.put(buffer.toString());

                    String response = outputBuffer.poll(10, TimeUnit.MILLISECONDS);
                    if (response != null) {
                        out.write(response);
                        out.flush();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
