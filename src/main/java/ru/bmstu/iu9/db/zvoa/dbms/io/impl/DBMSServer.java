package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

public class DBMSServer extends AbstractDbModule implements Consumer<HttpResponse>, Supplier<HttpRequest> {
    private Logger logger = Logger.getLogger(getClass().getName());

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap serverBootstrap;

    private Map<Connection, BlockingQueue<HttpRequest>> requests = new ConcurrentHashMap<>();
    private Map<Connection, BlockingQueue<HttpResponse>> responses = new ConcurrentHashMap<>();

    private ConcurrentSkipListMap<HttpRequest, Connection> liveQueue = new ConcurrentSkipListMap<>((o1, o2) -> {
        if (o1.getTimestamp() < o2.getTimestamp()) {
            return 1;
        } else if (o1.getTimestamp() > o2.getTimestamp()) {
            return -1;
        } else {
            return 0;
        }
    });

    private int port;

    public DBMSServer(int port) {
        this.port = port;
    }

    @Override
    public void accept(HttpResponse response) {
        try {
            Connection connection = response.getConnection();
            logger.info("Response " + response + " accept by OutputModule.");
            responses.get(connection).put(response);
            connection.notifyAll();
        } catch (InterruptedException e) {
        }
    }

    @Override
    public HttpRequest get() {
        try {
            Map.Entry<HttpRequest, Connection> entry = liveQueue.pollFirstEntry();

            if (entry != null) {
                BlockingQueue<HttpRequest> httpRequests
                        = requests
                        .get(entry.getValue());

                if (httpRequests != null) {
                    HttpRequest httpRequest = httpRequests.poll(10, TimeUnit.MILLISECONDS);

                    if (httpRequest != null) {
                        logger.info("Request " + httpRequest + " get by InputModule.");
                        return httpRequest;
                    }
                }
            }

            return null;
        } catch (InterruptedException e) {
            return null;
        }
    }

    public void init() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new DiscardServerHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

    }

    public void run() {
        try {
            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    public class DiscardServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            requests.put(new Connection(ctx), new LinkedBlockingQueue<>());
            responses.put(new Connection(ctx), new LinkedBlockingQueue<>());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            requests.remove(new Connection(ctx));
            responses.remove(new Connection(ctx), new LinkedBlockingQueue<>());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception {
            ((ByteBuf) msg).release();

            Connection connection = new Connection(ctx);
            HttpRequest request = new HttpRequest("request " + Instant.now().getEpochSecond(),
                    connection, Instant.now().toEpochMilli());

            logger.info("Put request " + request + " http server.");

            BlockingQueue queue = requests.get(connection);
            queue.put(request);

            DBMSServer.this.liveQueue.put(request, connection);

            HttpResponse httpResponse = responses.get(connection).poll(10, TimeUnit.MILLISECONDS);
            while (httpResponse == null) {
                synchronized (connection) {
                    try {
                        connection.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                httpResponse = responses.get(connection).poll(10, TimeUnit.MILLISECONDS);
            }

            logger.info("Send response " + httpResponse + " http server.");
            ctx.writeAndFlush(httpResponse.getResponse()).addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}