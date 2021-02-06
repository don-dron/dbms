package ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.netty.buffer.ByteBufUtil.appendPrettyHexDump;
import static io.netty.util.internal.StringUtil.NEWLINE;

/**
 * The type DBMS server - http server for SQL Requests handling.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class DBMSServer extends AbstractDbModule implements Consumer<HttpResponse>, Supplier<HttpRequest> {
    private final Logger logger = LoggerFactory.getLogger(DBMSServer.class);
    private final AtomicInteger requestsCount = new AtomicInteger();
    private final AtomicInteger responseCount = new AtomicInteger();
    private final Map<ConnectionInformation, BlockingQueue<HttpRequest>> requests = new ConcurrentHashMap<>();
    private final Map<ConnectionInformation, BlockingQueue<HttpResponse>> responses = new ConcurrentHashMap<>();
    private final ConcurrentSkipListMap<HttpRequest, ConnectionInformation> liveQueue = new ConcurrentSkipListMap<>(new RequestComparator());
    private final int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap serverBootstrap;
    private ChannelFuture serverFuture;

    /**
     * Instantiates a new DBMS server.
     *
     * @param port the port - listening port
     */
    public DBMSServer(int port) {
        this.port = port;
    }

    @Override
    public void accept(HttpResponse response) {
        try {
            ConnectionInformation connectionInformation = response.getConnectionInformation();
            logger.debug("Response " + response + " accept by OutputModule.");
            responses.get(connectionInformation).put(response);
        } catch (InterruptedException exception) {
            logger.warn(exception.getMessage());
        }
    }

    @Override
    public HttpRequest get() {
        try {
            Map.Entry<HttpRequest, ConnectionInformation> entry = liveQueue.pollFirstEntry();

            if (entry != null) {
                BlockingQueue<HttpRequest> httpRequests
                        = requests.get(entry.getValue());

                if (httpRequests != null) {
                    HttpRequest httpRequest = httpRequests.poll(10, TimeUnit.MILLISECONDS);

                    if (httpRequest != null) {
                        logger.debug("Request " + httpRequest + " get by InputModule.");
                        return httpRequest;
                    }
                }
            } else if (!requests.isEmpty()) {
                HttpRequest httpRequest = requests
                        .entrySet()
                        .stream()
                        .findFirst()
                        .orElse(null)
                        .getValue()
                        .poll(10, TimeUnit.MILLISECONDS);

                if (httpRequest != null) {
                    logger.debug("Request " + httpRequest + " get by InputModule.");
                    return httpRequest;
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
                .handler(new LoggingHandler())
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
                logger.error(error.getMessage());
            }
        }

        try {
            serverFuture.channel().closeFuture().sync();
        } catch (InterruptedException error) {
            logger.error(error.getMessage());
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    @Override
    public void close() {
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
        protected void initChannel(SocketChannel channel) {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new HttpObjectAggregator(1024 * 1024));
            pipeline.addLast(new DBMSConnectionHandler());
        }
    }

    /**
     * The handler for DBMS Server, handles Http connections and SQL Requests.
     *
     * @author don-dron Zvorygin Andrey BMSTU IU-9
     */
    public class DBMSConnectionHandler extends ChannelInboundHandlerAdapter {
        private final Logger logger = LoggerFactory.getLogger(DBMSConnectionHandler.class);
        private ConnectionInformation connectionInformation;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            connectionInformation = new ConnectionInformation(ctx);
            requests.put(connectionInformation, new LinkedBlockingQueue<>());
            responses.put(connectionInformation, new LinkedBlockingQueue<>());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            requests.remove(connectionInformation);
            responses.remove(connectionInformation);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            FullHttpRequest fullHttpRequest = ((FullHttpRequest) msg).copy();
            ((FullHttpRequest) msg).release();

            ByteBuf byteBuf = fullHttpRequest.content();
            String content = new StringBuffer(byteBuf.getCharSequence(0, byteBuf.readableBytes(), StandardCharsets.UTF_8))
                    .toString();

            HttpRequest request = new HttpRequest(
                    UUID.randomUUID().toString(),
                    content,
                    connectionInformation,
                    Instant.now().toEpochMilli());

            while (true) {
                try {
                    requests.get(connectionInformation).put(request);
                    break;
                } catch (InterruptedException exception) {
                    logger.warn(exception.getMessage());
                }
            }
            DBMSServer.this.liveQueue.put(request, connectionInformation);
            requestsCount.getAndIncrement();
            logger.debug("Put request " + request + " http server.");

            HttpResponse httpResponse;
            while (true) {
                try {
                    httpResponse = responses.get(connectionInformation).poll(10, TimeUnit.MILLISECONDS);
                } catch (InterruptedException exception) {
                    logger.warn(exception.getMessage());
                    continue;
                }

                if (httpResponse == null) {
                    Thread.onSpinWait();
                } else {
                    break;
                }
            }

            ByteBuf responseContent = Unpooled.copiedBuffer(httpResponse.getResponse(), CharsetUtil.UTF_8);
            FullHttpResponse response =
                    new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, responseContent);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, responseContent.readableBytes());

            logger.debug("Send response " + response + " http server.");

            responseCount.getAndIncrement();
            ctx.writeAndFlush(response);

            logger.debug("Request count: " + requestsCount.get() + "\nResponse count: " + responseCount.get());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause != null) {
                logger.warn(cause.getMessage());
            }
            ctx.close();
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }
    }

    private static class RequestComparator implements Comparator<HttpRequest> {
        @Override
        public int compare(HttpRequest first, HttpRequest second) {
            if (first.getTimestamp() < second.getTimestamp()) {
                return 1;
            } else if (first.getTimestamp() > second.getTimestamp()) {
                return -1;
            } else {
                if (first.hashCode() < second.hashCode()) {
                    return 1;
                } else if (first.hashCode() > second.hashCode()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }
    }

    public static class LoggingHandler extends ChannelDuplexHandler {
        protected final Logger logger;

        public LoggingHandler() {
            logger = LoggerFactory.getLogger(getClass());
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            logger.debug(format(ctx, "REGISTERED"));
            ctx.fireChannelRegistered();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            logger.debug(format(ctx, "UNREGISTERED"));
            ctx.fireChannelUnregistered();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.debug(format(ctx, "ACTIVE"));
            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.debug(format(ctx, "INACTIVE"));
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.debug(format(ctx, "EXCEPTION"));
            ctx.fireExceptionCaught(cause);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            logger.debug(format(ctx, "USER_EVENT"));
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            logger.debug(format(ctx, "BIND"));
            ctx.bind(localAddress, promise);
        }

        @Override
        public void connect(
                ChannelHandlerContext ctx,
                SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            logger.debug(format(ctx, "CONNECT"));
            ctx.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            logger.debug(format(ctx, "DISCONNECT"));
            ctx.disconnect(promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            logger.debug(format(ctx, "CLOSE"));
            ctx.close(promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            logger.debug(format(ctx, "DEREGISTER"));
            ctx.deregister(promise);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            logger.debug(format(ctx, "READ COMPLETE"));
            ctx.fireChannelReadComplete();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            logger.debug(format(ctx, "READ"));
            ctx.fireChannelRead(msg);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            logger.debug(format(ctx, "WRITE"));
            ctx.write(msg, promise);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            logger.debug(format(ctx, "WRITABILITY CHANGED"));
            ctx.fireChannelWritabilityChanged();
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception {
            logger.debug(format(ctx, "FLUSH"));
            ctx.flush();
        }

        protected String format(ChannelHandlerContext ctx, String eventName) {
            String chStr = ctx.channel().toString();
            return new StringBuilder(chStr.length() + 1 + eventName.length())
                    .append(chStr)
                    .append(' ')
                    .append(eventName)
                    .toString();
        }

        protected String format(ChannelHandlerContext ctx, String eventName, Object arg) {
            if (arg instanceof ByteBuf) {
                return formatByteBuf(ctx, eventName, (ByteBuf) arg);
            } else if (arg instanceof ByteBufHolder) {
                return formatByteBufHolder(ctx, eventName, (ByteBufHolder) arg);
            } else {
                return formatSimple(ctx, eventName, arg);
            }
        }

        protected String format(ChannelHandlerContext ctx, String eventName, Object firstArg, Object secondArg) {
            if (secondArg == null) {
                return formatSimple(ctx, eventName, firstArg);
            }

            String chStr = ctx.channel().toString();
            String arg1Str = String.valueOf(firstArg);
            String arg2Str = secondArg.toString();
            StringBuilder buf = new StringBuilder(
                    chStr.length() + 1 + eventName.length() + 2 + arg1Str.length() + 2 + arg2Str.length());
            buf.append(chStr).append(' ').append(eventName).append(": ").append(arg1Str).append(", ").append(arg2Str);
            return buf.toString();
        }

        private static String formatByteBuf(ChannelHandlerContext ctx, String eventName, ByteBuf msg) {
            String chStr = ctx.channel().toString();
            int length = msg.readableBytes();
            if (length == 0) {
                StringBuilder buf = new StringBuilder(chStr.length() + 1 + eventName.length() + 4);
                buf.append(chStr).append(' ').append(eventName).append(": 0B");
                return buf.toString();
            } else {
                int rows = length / 16 + (length % 15 == 0 ? 0 : 1) + 4;
                StringBuilder buf = new StringBuilder(chStr.length() + 1 + eventName.length() + 2 + 10 + 1 + 2 + rows * 80);

                buf.append(chStr).append(' ').append(eventName).append(": ").append(length).append('B').append(NEWLINE);
                appendPrettyHexDump(buf, msg);

                return buf.toString();
            }
        }

        private static String formatByteBufHolder(ChannelHandlerContext ctx, String eventName, ByteBufHolder msg) {
            String chStr = ctx.channel().toString();
            String msgStr = msg.toString();
            ByteBuf content = msg.content();
            int length = content.readableBytes();
            if (length == 0) {
                StringBuilder buf = new StringBuilder(chStr.length() + 1 + eventName.length() + 2 + msgStr.length() + 4);
                buf.append(chStr).append(' ').append(eventName).append(", ").append(msgStr).append(", 0B");
                return buf.toString();
            } else {
                int rows = length / 16 + (length % 15 == 0 ? 0 : 1) + 4;
                StringBuilder buf = new StringBuilder(
                        chStr.length() + 1 + eventName.length() + 2 + msgStr.length() + 2 + 10 + 1 + 2 + rows * 80);

                buf.append(chStr).append(' ').append(eventName).append(": ")
                        .append(msgStr).append(", ").append(length).append('B').append(NEWLINE);
                appendPrettyHexDump(buf, content);

                return buf.toString();
            }
        }

        private static String formatSimple(ChannelHandlerContext ctx, String eventName, Object msg) {
            String chStr = ctx.channel().toString();
            String msgStr = String.valueOf(msg);
            StringBuilder buf = new StringBuilder(chStr.length() + 1 + eventName.length() + 2 + msgStr.length());
            return buf.append(chStr).append(' ').append(eventName).append(": ").append(msgStr).toString();
        }
    }
}