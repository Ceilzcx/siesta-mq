package com.ceilzcx.siestamq.remoting.netty;

import com.ceilzcx.siestamq.common.Pair;
import com.ceilzcx.siestamq.common.utils.NetworkUtil;
import com.ceilzcx.siestamq.remoting.ChannelEventListener;
import com.ceilzcx.siestamq.remoting.InvokeCallback;
import com.ceilzcx.siestamq.remoting.RemotingServer;
import com.ceilzcx.siestamq.remoting.common.RemotingHelper;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTooMuchRequestException;
import com.ceilzcx.siestamq.remoting.netty.config.NettyServerConfig;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ceilzcx
 * @since 8/12/2022
 * <p>
 * Netty Server <---> NettyServerHandler (receive and read msg) <---> processMessageReceived (处理request and response)
 *                         invokeSync <---> client <---> write to channel <--'
 */
@Slf4j
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    public static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";

    /**
     * key: port, value: netty server
     */
    private final Map<Integer, NettyRemotingAbstract> remotingServerTable = new ConcurrentHashMap<>();

    private final NettyServerConfig nettyServerConfig;
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workGroup;

    private HandshakeHandler handshakeHandler;
    private NettyServerHandler serverHandler;
    private NettyConnectManageHandler connectionManageHandler;
    private RemotingCodeDistributionHandler distributionHandler;

    // netty/boss中使用的eventExecutorGroup
    private EventExecutorGroup defaultEventExecutorGroup;

    private final ExecutorService publicExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ChannelEventListener channelEventListener;

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
                               final ChannelEventListener channelEventListener) {
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());

        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        this.serverBootstrap = new ServerBootstrap();
        this.bossGroup = this.buildBossGroup();
        this.workGroup = this.buildWorkGroup();

        this.publicExecutor = buildPublicExecutor(this.nettyServerConfig);
        this.scheduledExecutorService = buildScheduleExecutor();
    }

    private EventLoopGroup buildBossGroup() {
        if (this.useEpoll()) {
            return new EpollEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyEPOLLBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });
        } else {
            return new NioEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyNIOBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });
        }
    }

    private EventLoopGroup buildWorkGroup() {
        if (this.useEpoll()) {
            return new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                private final int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerEPOLLSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        } else {
            return new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                private final int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        }
    }

    private ExecutorService buildPublicExecutor(NettyServerConfig nettyServerConfig) {
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        return Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
    }

    private ScheduledExecutorService buildScheduleExecutor() {
        return new ScheduledThreadPoolExecutor(1,
                r -> {
                    Thread thread = new Thread(r, "NettyServerScheduler");
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    private boolean useEpoll() {
        return NetworkUtil.isLinuxPlatform()
                && this.nettyServerConfig.isUseEpollNativeSelector()
                && Epoll.isAvailable();
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executorService) {
        ExecutorService executorThis = executorService;
        if (null == executorThis) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executorService) {
        this.defaultProcessor = new Pair<>(processor, executorService);
    }

    @Override
    public int localListenPort() {
        return this.nettyServerConfig.getListenPort();
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return this.processorTable.get(requestCode);
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
        return this.defaultProcessor;
    }

    @Override
    // 多个端口的时候, 为什么需要监听多个端口?
    public RemotingServer newRemotingServer(int port) {
        if (this.remotingServerTable.containsKey(port)) {
            throw new RuntimeException("The port " + port + " already in use by another RemotingServer");
        }
        SubRemotingServer subRemotingServer = new SubRemotingServer(
                port,
                this.nettyServerConfig.getServerAsyncSemaphoreValue(),
                this.nettyServerConfig.getServerOnewaySemaphoreValue());
        this.remotingServerTable.put(port, subRemotingServer);
        return subRemotingServer;
    }

    @Override
    public void removeRemotingServer(int port) {
        this.remotingServerTable.remove(port);
    }

    @Override
    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                this.nettyServerConfig.getServerWorkerThreads(),
                new ThreadFactory() {

                    private final AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                    }
                });

        this.prepareHandlers();

        this.serverBootstrap.group(this.bossGroup, this.workGroup)
                // netty自己实现的Epoll channel, 而不是linux的epoll, 有挺大的改动
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getBindAddress(),
                        this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
//                                .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, handshakeHandler)
                                .addLast(defaultEventExecutorGroup,
                                        new NettyEncoder(),
                                        new NettyDecoder(),
                                        distributionHandler,
                                        new IdleStateHandler(0, 0,
                                                nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                        connectionManageHandler,
                                        serverHandler
                                );
                    }
                });

        this.addCustomConfig(this.serverBootstrap);

        try {
            ChannelFuture future = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) future.channel().localAddress();
            if (0 == this.nettyServerConfig.getListenPort()) {
                this.nettyServerConfig.setListenPort(addr.getPort());
            }

            log.info("RemotingServer started, listening {}:{}", this.nettyServerConfig.getBindAddress(),
                    this.nettyServerConfig.getListenPort());

            this.remotingServerTable.put(this.nettyServerConfig.getListenPort(), this);
        } catch (Exception e) {
            // log
        }

        if (this.channelEventListener != null) {
//            this.nettyEventExecutor.start();
        }
    }

    // 创建pipeline需要的所有handler
    private void prepareHandlers() {
        this.handshakeHandler = new HandshakeHandler();
        this.distributionHandler = new RemotingCodeDistributionHandler();
        this.connectionManageHandler = new NettyConnectManageHandler();
        this.serverHandler = new NettyServerHandler();
    }

    // todo 用户设置handler默认项
    private void addCustomConfig(ServerBootstrap serverBootstrap) {

    }

    @Override
    public void shutdown() {

        this.bossGroup.shutdownGracefully();

        this.workGroup.shutdownGracefully();

        if (this.defaultEventExecutorGroup != null) {
            this.defaultEventExecutorGroup.shutdownGracefully();
        }
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }


    /**
     * TLS协议实现
     */
    @ChannelHandler.Sharable
    public class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        }
    }

    /**
     * 该channel主要处理消息, 调用NettyRemotingAbstract.processMessageReceived
     */
    @ChannelHandler.Sharable
    public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            int localPort = RemotingHelper.parseSocketAddressPort(ctx.channel().localAddress());
            NettyRemotingAbstract remotingAbstract = NettyRemotingServer.this.remotingServerTable.get(localPort);
            if (remotingAbstract != null && localPort != -1) {
                remotingAbstract.processMessageReceived(ctx, msg);
                return;
            }
            RemotingHelper.closeChannel(ctx.channel());
        }

        // todo 不太理解, 应该是为了处理当channel不可写时, 也不处理读消息
        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            Channel channel = ctx.channel();
            if (channel.isWritable()) {
                if (!channel.config().isAutoRead()) {
                    channel.config().setAutoRead(true);
//                    log.info("Channel[{}] turns writable, bytes to buffer before changing channel to un-writable: {}",
//                            RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeUnwritable());
                }
            } else {
                channel.config().setAutoRead(false);
//                log.warn("Channel[{}] auto-read is disabled, bytes to drain before it turns writable: {}",
//                        RemotingHelper.parseChannelRemoteAddr(channel), channel.bytesBeforeWritable());
            }
            super.channelWritabilityChanged(ctx);
        }
    }

    /**
     * 打印连接日志, 同时通过event事件告诉其他模块, 有新的连接
     */
    @ChannelHandler.Sharable
    public class NettyConnectManageHandler extends ChannelDuplexHandler {

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            // log
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            // log
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
        }
    }

    class SubRemotingServer extends NettyRemotingAbstract implements RemotingServer {
        private final int listenPort;

        protected SubRemotingServer(final int port, final int permitsOneway, final int permitsAsync) {
            super(permitsOneway, permitsAsync);
            this.listenPort = port;
        }

        @Override
        public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executorService) {
            // 不能直接使用NettyRemotingServer.this.registerProcessor, 不是同一个processorTable
            ExecutorService executorThis = executorService;
            if (null == executorService) {
                executorThis = NettyRemotingServer.this.publicExecutor;
            }

            Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
            this.processorTable.put(requestCode, pair);
        }

        @Override
        public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executorService) {
            this.defaultProcessor = new Pair<>(processor, executorService);
        }

        @Override
        public int localListenPort() {
            return listenPort;
        }

        @Override
        public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
            return this.processorTable.get(requestCode);
        }

        @Override
        public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
            return this.defaultProcessor;
        }

        @Override
        public RemotingServer newRemotingServer(int port) {
            throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer " +
                    "doesn't support new nested RemotingServer");
        }

        @Override
        public void removeRemotingServer(int port) {
            throw new UnsupportedOperationException("The SubRemotingServer of NettyRemotingServer " +
                    "doesn't support remove nested RemotingServer");
        }

        @Override
        public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis)
                throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
            return null;
        }

        @Override
        public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
                throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException {

        }

        @Override
        public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis)
                throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException {

        }

        @Override
        public void start() {

        }

        @Override
        public void shutdown() {

        }

        @Override
        public ExecutorService getCallbackExecutor() {
            return NettyRemotingServer.this.getCallbackExecutor();
        }
    }
}
