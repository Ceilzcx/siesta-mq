package com.ceilzcx.siesta.nameserver.routeinfo;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ceilzcx
 * @since 20/12/2022
 */
public class RouteInfoManager {

    private final Map<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    public RouteInfoManager() {
        this.clusterAddrTable = new ConcurrentHashMap<>(32);
    }

}
