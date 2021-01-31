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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

public class DBMSServer extends AbstractDbModule
        implements Consumer<HttpResponse>, Supplier<HttpRequest> {
    private final static Logger LOGGER = LoggerFactory.getLogger(DBMSServer.class);

    private final Map<Connection, BlockingQueue<HttpRequest>> requests = new ConcurrentHashMap<>();
    private final Map<Connection, BlockingQueue<HttpResponse>> responses = new ConcurrentHashMap<>();
    private final ConcurrentSkipListMap<HttpRequest, Connection> liveQueue = new ConcurrentSkipListMap<>((o1, o2) -> {
        if (o1.getTimestamp() < o2.getTimestamp()) {
            return 1;
        } else if (o1.getTimestamp() > o2.getTimestamp()) {
            return -1;
        } else {
            return 0;
        }
    });

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap serverBootstrap;
    private ChannelFuture serverFuture;
    private int port;

    public DBMSServer(int port) {
        this.port = port;
    }

    @Override
    public void accept(HttpResponse response) {
        try {
            Connection connection = response.getConnection();
            LOGGER.info("Response " + response + " accept by OutputModule.");
            responses.get(connection).put(response);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public HttpRequest get() {
        try {
            Map.Entry<HttpRequest, Connection> entry = liveQueue.pollFirstEntry();

            if (entry != null) {
                BlockingQueue<HttpRequest> httpRequests
                        = requests.get(entry.getValue());

                if (httpRequests != null) {
                    HttpRequest httpRequest = httpRequests.poll(10, TimeUnit.MILLISECONDS);

                    if (httpRequest != null) {
                        LOGGER.info("Request " + httpRequest + " get by InputModule.");

                        return httpRequest;
                    }
                }
            }

            return null;
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public void init() {
        synchronized (this) {
            if (isInit()) {
                return;
            }
            setInit();
            logInit();
        }

        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new HttpServerInitializer())
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

    }

    @Override
    public void run() {
        synchronized (this) {
            if (isRunning()) {
                return;
            }
            setRunning();
            logRunning();

            try {
                serverFuture = serverBootstrap.bind(port).sync();
            } catch (InterruptedException error) {
                LOGGER.error(error.getMessage());
            }
        }

        try {
            serverFuture.channel().closeFuture().sync();
        } catch (InterruptedException error) {
            LOGGER.error(error.getMessage());
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            setClosed();
            logClose();
        }

        if (serverFuture != null) {
            ChannelFuture future = serverFuture.channel().close();
            future.awaitUninterruptibly();
        }
    }

    public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new HttpObjectAggregator(1024 * 1024));
            pipeline.addLast(new DiscardServerHandler());
        }
    }

    public class DiscardServerHandler extends ChannelInboundHandlerAdapter {
        private Logger logger = LoggerFactory.getLogger(DiscardServerHandler.class);
        private Connection connection;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            connection = new Connection(ctx);
            requests.put(connection, new LinkedBlockingQueue<>());
            responses.put(connection, new LinkedBlockingQueue<>());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            requests.remove(connection);
            responses.remove(connection);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception {
            try {
                HttpRequest request = new HttpRequest("request " + Instant.now().getEpochSecond(),
                        connection, Instant.now().toEpochMilli());
                logger.info("Put request " + request + " http server.");

                BlockingQueue queue = requests.get(connection);
                queue.put(request);

                DBMSServer.this.liveQueue.put(request, connection);

                HttpResponse httpResponse = responses.get(connection).poll(10, TimeUnit.MILLISECONDS);
                while (httpResponse == null) {
                    Thread.yield();
                    httpResponse = responses.get(connection).poll(10, TimeUnit.MILLISECONDS);
                }

                ByteBuf content = Unpooled.copiedBuffer(httpResponse.getResponse(), CharsetUtil.UTF_8);
                FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html");
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

                logger.info("Send response " + response + " http server.");
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            } catch (Exception exception) {
                logger.warn(exception.getMessage());
            } finally {
                ((ByteBuf) msg).release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause != null) {
                logger.warn(cause.getMessage());
            }
            ctx.close();
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }
    }
}