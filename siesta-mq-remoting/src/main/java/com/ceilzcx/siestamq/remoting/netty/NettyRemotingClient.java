package com.ceilzcx.siestamq.remoting.netty;

import com.ceilzcx.siestamq.common.Pair;
import com.ceilzcx.siestamq.remoting.ChannelEventListener;
import com.ceilzcx.siestamq.remoting.InvokeCallback;
import com.ceilzcx.siestamq.remoting.RemotingClient;
import com.ceilzcx.siestamq.remoting.common.RemotingHelper;
import com.ceilzcx.siestamq.remoting.exception.RemotingConnectException;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTooMuchRequestException;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import com.ceilzcx.siestamq.remoting.protocol.RequestCode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.extern.slf4j.Slf4j;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author ceilzcx
 * @since 13/12/2022
 */
@Slf4j
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroupWorker;

    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    // key: address, value: channel + responseTime
    private final ConcurrentMap<String, ChannelWrapper> channelTable = new ConcurrentHashMap<>();
    private final AtomicReference<List<String>> nameserverAddrList = new AtomicReference<>();
    // todo why need this object?
    private final AtomicReference<String> nameserverAddrChoose = new AtomicReference<>();

    private final ConcurrentMap<String, Boolean> availableNameserverAddrTable = new ConcurrentHashMap<>();

    private final Lock lockChannelTable = new ReentrantLock();
    private final Lock nameserverChannelLock = new ReentrantLock();
    // todo 源码使用初始值为随机0~999, 是否有必要
    private final AtomicInteger nameserverIndex = new AtomicInteger(0);

    private final ExecutorService publicExecutor;
    private final ExecutorService scanExecutor;

    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
                               final ChannelEventListener channelEventListener) {
        this(nettyClientConfig, channelEventListener, null, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
                               final ChannelEventListener channelEventListener,
                               final EventLoopGroup eventLoopGroup,
                               final EventExecutorGroup eventExecutorGroup) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;

        this.bootstrap = new Bootstrap();
        // todo 为什么client不需要判断使用EpollEventLoopGroup?
        if (eventLoopGroup != null) {
            this.eventLoopGroupWorker = eventLoopGroup;
        } else {
            this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
                }
            });
        }


        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });


        this.scanExecutor = new ThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(32), new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientScan_thread_" + this.threadIndex.incrementAndGet());
            }
        });
    }

    @Override
    public void updateNameServerAddrList(List<String> addrList) {
        List<String> oldAddrList = this.nameserverAddrList.get();
        boolean update = false;
        if (!addrList.isEmpty()) {
            if (null == oldAddrList || oldAddrList.size() != addrList.size()) {
                update = true;
            } else {
                for (String addr : addrList) {
                    if (!oldAddrList.contains(addr)) {
                        update = true;
                        break;
                    }
                }
            }

            if (update) {
                // why?
                Collections.shuffle(addrList);
                this.nameserverAddrList.set(addrList);

                // should close the channel if choose addr is not exist.
                String chooseAddr = this.nameserverAddrChoose.get();
                if ( chooseAddr != null && !addrList.contains( chooseAddr)) {
                    for (Map.Entry<String, ChannelWrapper> entry : this.channelTable.entrySet()) {
                        if (entry.getKey().contains(chooseAddr)) {
                            ChannelWrapper channelWrapper = entry.getValue();
                            if (channelWrapper != null) {
                                this.closeChannel(channelWrapper.getChannel());
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public List<String> getNameServerAddressList() {
        return nameserverAddrList.get();
    }

    @Override
    public List<String> getAvailableNameServerList() {
        return new ArrayList<>(this.availableNameserverAddrTable.keySet());
    }

    // 定时跑, 查找可靠的address
    private void scanAvailableNameServerList() {
        List<String> addrList = this.nameserverAddrList.get();
        if (addrList == null || addrList.isEmpty()) {
            return;
        }
        for (String addr : nameserverAddrList.get()) {
            this.scanExecutor.execute(() -> {
                try {

                    Channel channel = this.getOrCreateChannel(addr);
                    if (channel != null) {
                        NettyRemotingClient.this.availableNameserverAddrTable.put(addr, true);
                    } else {
                        NettyRemotingClient.this.availableNameserverAddrTable.remove(addr);
                    }
                } catch (Exception e) {
                    log.error("scanAvailableNameSrv get channel of {} failed, ", addr, e);
                }
            });
        }
    }

    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginStartTime = System.currentTimeMillis();
        Channel channel = this.getOrCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                this.doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTimeoutException("invokeSync call the addr[" + addr + "] timeout");
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);
                this.doAfterRpcHooks(addr, request, response);
                this.updateChannelLastResponseTime(addr);
                return response;
            } catch (RemotingSendRequestException e) {
                log.error("invoke sync error, error msg: {}", e.getMessage());
                throw e;
            }
        } else {
            this.createChannel(addr);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getOrCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                this.doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTooMuchRequestException("invokeAsync call the addr[" + addr + "] timeout");
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, new InvokeCallbackWrapper(invokeCallback, addr));
            } catch (RemotingSendRequestException e) {
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        Channel channel = this.getOrCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            this.doBeforeRpcHooks(addr, request);
            this.invokeOnewayImpl(channel, request, timeoutMillis);
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    private Channel getOrCreateChannel(String addr) throws InterruptedException {
        if (addr == null) {
            return getAndCreateNameserverChannel();
        }
        ChannelWrapper channelWrapper = this.channelTable.get(addr);
        if (channelWrapper != null && channelWrapper.isOK()) {
            return channelWrapper.getChannel();
        }
        return this.createChannel(addr);
    }

    // todo 相比源码少了proxy部分, 应该是代理的意思, 逻辑比较复杂
    // 通过addr获取channel, channel用于invoke部分发送消息使用
    private Channel createChannel(final String addr) {
        ChannelWrapper channelWrapper = this.channelTable.get(addr);
        if (channelWrapper != null && channelWrapper.isOK()) {
            return channelWrapper.getChannel();
        }

        try {
            if (this.lockChannelTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                channelWrapper = this.channelTable.get(addr);
                boolean needCreateNewConnection;
                if (channelWrapper != null) {
                    if (channelWrapper.isOK()) {
                        return channelWrapper.getChannel();
                    } else if (!channelWrapper.getChannelFuture().isDone()) {
                        needCreateNewConnection = false;
                    } else {
                        this.channelTable.remove(addr);
                        needCreateNewConnection = true;
                    }
                } else {
                    needCreateNewConnection = true;
                }
                if (needCreateNewConnection) {
                    String[] hostAndPort = this.getHostAndPort(addr);
                    ChannelFuture future = bootstrap.connect(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                    channelWrapper = new ChannelWrapper(future);
                    this.channelTable.put(addr, channelWrapper);
                }
            }
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        }

        if (channelWrapper != null) {
            if (channelWrapper.getChannelFuture().awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (channelWrapper.isOK()) {
                    return channelWrapper.getChannel();
                } else {
                    // log
                }
            }
        }

        return null;
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel) {
            return;
        }

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTable.get(addrRemote);

                    if (null == prevCW) {
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTable.remove(addrRemote);
                    }

                    RemotingHelper.closeChannel(channel);
                } catch (Exception e) {
                } finally {
                    this.lockChannelTable.unlock();
                }
            } else {
            }
        } catch (InterruptedException e) {
        }
    }

    private void closeChannel(final Channel channel) {
        if (null == channel) {
            return;
        }
        try {
            if (this.lockChannelTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                String addr = null;
                // 不知道为什么没有查到table对应的channel就不关闭, 相比源码有所改动
                // for循环遍历的目的是为了找到addr, 将channel从channelTable里面删除
                for (Map.Entry<String, ChannelWrapper> entry : this.channelTable.entrySet()) {
                    ChannelWrapper channelWrapper = entry.getValue();
                    if (channelWrapper != null && channelWrapper.getChannel() == channel) {
                        addr = entry.getKey();
                    }
                }
                if (addr != null) {
                    this.channelTable.remove(addr);
                    RemotingHelper.closeChannel(channel);
                }
            }
        } catch (Exception e) {

        }
    }

    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        String addr = this.nameserverAddrChoose.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTable.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannel();
            }
        }

        final List<String> addrList = this.nameserverAddrList.get();
        if (this.nameserverChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                addr = this.nameserverAddrChoose.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTable.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.nameserverIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();
                        String newAddr = addrList.get(index);

                        this.nameserverAddrChoose.set(newAddr);
                        log.info("new name server is chosen. OLD: {} , NEW: {}. nameserverIndex = {}", addr, newAddr, nameserverIndex);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                    throw new RemotingConnectException(addrList.toString());
                }
            } catch (Exception e) {
                log.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.nameserverChannelLock.unlock();
            }
        } else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    private void updateChannelLastResponseTime(final String addr) {
        String address = addr;
        if (address == null) {
            address = this.nameserverAddrChoose.get();
        }
        if (address == null) {
            return;
        }
        ChannelWrapper channelWrapper = this.channelTable.get(address);
        if (channelWrapper != null && channelWrapper.isOK()) {
            channelWrapper.updateLastResponseTime();
        }
    }

    // Do not use RemotingUtil, it will directly resolve the domain
    private String[] getHostAndPort(String address) {
        return address.split(":");
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executorService) {
        ExecutorService executorThis = executorService;
        if (null == executorService) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void setCallbackExecutor(ExecutorService executorService) {

    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper channelWrapper = this.channelTable.get(addr);
        return channelWrapper != null && channelWrapper.isOK() && channelWrapper.isWritable();
    }

    @Override
    public void closeChannels(List<String> addrList) {
        for (String addr : addrList) {
            ChannelWrapper channelWrapper = this.channelTable.get(addr);
            if (channelWrapper != null) {
                this.closeChannel(channelWrapper.getChannel());
            }
        }
        this.interruptPullRequests(new HashSet<>(addrList));
    }

    // todo 几个疑惑
    // 为什么只终止pull的操作
    // 为什么只在closeChannels中调用, 而不是直接在closeChannel中调用
    private void interruptPullRequests(Set<String> brokerAddrSet) {
        for (ResponseFuture responseFuture : responseTable.values()) {
            RemotingCommand request = responseFuture.getRequest();
            if (request == null) {
                continue;
            }
            String remoteAddr = RemotingHelper.parseChannelRemoteAddr(responseFuture.getChannel());
            if (brokerAddrSet.contains(remoteAddr) &&
                    (request.getCode() == RequestCode.PULL_MESSAGE ||
                    request.getCode() == RequestCode.LITE_PULL_MESSAGE)) {
                responseFuture.interrupt();
            }
        }
    }

    @Override
    public void start() {
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                    this.nettyClientConfig.getClientWorkerThreads(),
                    new ThreadFactory() {
                        private final AtomicInteger threadIndex = new AtomicInteger(0);

                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                        }
                    });
        }

        // 设置bootstrap
        this.bootstrap.group(this.eventLoopGroupWorker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new NettyConnectManageHandler(),
                                new NettyClientHandler());
                    }
                });

        this.addCustomConfig(bootstrap);

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                NettyRemotingClient.this.scanAvailableNameServerList();
            }
        }, 0, this.nettyClientConfig.getConnectTimeoutMillis());
    }

    // todo 用户设置handler默认项
    private void addCustomConfig(Bootstrap bootstrap) {

    }

    @Override
    public void shutdown() {
        for (Map.Entry<String, ChannelWrapper> entry : this.channelTable.entrySet()) {
            this.closeChannel(entry.getValue().getChannel());
        }
        this.channelTable.clear();
        this.eventLoopGroupWorker.shutdownGracefully();
        if (this.nettyEventExecutor != null) {
//            this.nettyEventExecutor
        }
        if (this.defaultEventExecutorGroup != null) {
            this.defaultEventExecutorGroup.shutdownGracefully();
        }
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return null;
    }


    class InvokeCallbackWrapper implements InvokeCallback {

        private final InvokeCallback invokeCallback;
        private final String addr;

        public InvokeCallbackWrapper(InvokeCallback invokeCallback, String addr) {
            this.invokeCallback = invokeCallback;
            this.addr = addr;
        }

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            if (responseFuture != null && responseFuture.isSendRequestOK() && responseFuture.getResponse() != null) {
                NettyRemotingClient.this.updateChannelLastResponseTime(addr);
            }
            this.invokeCallback.operationComplete(responseFuture);
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    // todo
    class NettyConnectManageHandler extends ChannelDuplexHandler {

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            super.connect(ctx, remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            super.disconnect(ctx, promise);
        }
    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;
        // only affected by sync or async request, oneway is not included.
        private long lastResponseTime;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
            this.lastResponseTime = System.currentTimeMillis();
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }

        public long getLastResponseTime() {
            return this.lastResponseTime;
        }

        public void updateLastResponseTime() {
            this.lastResponseTime = System.currentTimeMillis();
        }
    }


}
