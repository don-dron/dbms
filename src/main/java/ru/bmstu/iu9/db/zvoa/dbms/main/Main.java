package ru.bmstu.iu9.db.zvoa.dbms.main;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
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

/**
 * The type Main.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class Main {
    private static Supplier<BlockingQueue> queueSupplier = () -> new LinkedBlockingQueue();
    private static String ADDRESS = "127.0.0.1";
    private static int PORT = 38900;

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

        dbms.init();
        httpServer.init();

        Thread dbmsThread = new Thread(dbms::run);
        dbmsThread.start();
        Thread serverThread = new Thread(httpServer::run);
        serverThread.start();

        Thread.sleep(3600000);

        httpServer.close();
        dbms.close();
        dbmsThread.join();
        serverThread.join();
    }
}
