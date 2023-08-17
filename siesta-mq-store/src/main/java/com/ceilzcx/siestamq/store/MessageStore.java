package com.ceilzcx.siestamq.store;

/**
 * @author ceilzcx
 * @since 21/12/2022
 */
public interface MessageStore {

    boolean load();

    void start() throws Exception;

    void shutdown();

    void destroy();


}
