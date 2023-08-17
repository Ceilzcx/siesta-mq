package com.ceilzcx.siesta.nameserver;

import com.ceilzcx.siestamq.common.nameserver.NameserverConfig;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;
import com.ceilzcx.siestamq.remoting.netty.config.NettyServerConfig;

/**
 * @author ceilzcx
 * @since 16/12/2022
 */
public class NameserverStartup {
    private static NameserverConfig nameserverConfig;
    private static NettyServerConfig nettyServerConfig;
    private static NettyClientConfig nettyClientConfig;

    public static void main(String[] args) {
        init();
        createAndStartController();
    }

    public static void init() {
        nameserverConfig = new NameserverConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);
        nettyClientConfig = new NettyClientConfig();
    }

    public static void createAndStartController() {
        NameserverController controller = new NameserverController(
                nameserverConfig,
                nettyServerConfig,
                nettyClientConfig
        );
        boolean result = controller.initialize();
        if (!result) {
            controller.shutdown();
            System.exit(-3);
        }
        controller.start();
    }

}
