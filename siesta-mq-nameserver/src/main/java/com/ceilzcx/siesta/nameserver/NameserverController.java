package com.ceilzcx.siesta.nameserver;

import com.ceilzcx.siesta.nameserver.processor.DefaultRequestProcessor;
import com.ceilzcx.siestamq.common.nameserver.NameserverConfig;
import com.ceilzcx.siestamq.common.utils.NetworkUtil;
import com.ceilzcx.siestamq.remoting.RemotingClient;
import com.ceilzcx.siestamq.remoting.RemotingServer;
import com.ceilzcx.siestamq.remoting.netty.NettyRemotingClient;
import com.ceilzcx.siestamq.remoting.netty.NettyRemotingServer;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;
import com.ceilzcx.siestamq.remoting.netty.config.NettyServerConfig;

import java.util.Collections;
import java.util.concurrent.*;

/**
 * @author ceilzcx
 * @since 15/12/2022
 */
public class NameserverController {
    private final NameserverConfig nameserverConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    private RemotingServer remotingServer;
    private RemotingClient remotingClient;

    private ExecutorService defaultExecutor;

    private BlockingQueue<Runnable> defaultThreadPoolQueue;

    public NameserverController(NameserverConfig nameserverConfig,
                                NettyServerConfig nettyServerConfig,
                                NettyClientConfig nettyClientConfig) {
        this.nameserverConfig = nameserverConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
    }

    public boolean initialize() {
        this.initThreadExecutors();
        this.initNetworkComponents();
        this.registerProcessor();
        return true;
    }

    public void start() {
        this.remotingServer.start();
        if (0 == this.nettyServerConfig.getListenPort()) {
            this.nettyServerConfig.setListenPort(this.remotingServer.localListenPort());
        }
        this.remotingClient.updateNameServerAddrList(Collections.singletonList(
                NetworkUtil.getLocalAddress() + ":" + this.nettyServerConfig.getListenPort()
        ));
        this.remotingClient.start();
    }

    public void shutdown() {
        this.defaultThreadPoolQueue.clear();
        this.remotingServer.shutdown();
        this.remotingClient.shutdown();
        this.defaultExecutor.shutdown();
    }

    public void initNetworkComponents() {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);
        this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
    }

    public void initThreadExecutors() {
        this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(this.nameserverConfig.getDefaultThreadPoolQueueCapacity());
        this.defaultExecutor = new ThreadPoolExecutor(
                this.nameserverConfig.getDefaultThreadPoolNums(),
                this.nameserverConfig.getDefaultThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.defaultThreadPoolQueue);
    }

    private void registerProcessor() {
        // other

        this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.defaultExecutor);
    }

}
