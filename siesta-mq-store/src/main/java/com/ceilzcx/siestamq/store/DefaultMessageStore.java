package com.ceilzcx.siestamq.store;

import com.ceilzcx.siestamq.store.config.MessageStoreConfig;

/**
 * @author ceilzcx
 * @since 23/12/2022
 */
public class DefaultMessageStore implements MessageStore {
    private final MessageStoreConfig messageStoreConfig;

    public DefaultMessageStore(MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
    }

    @Override
    public boolean load() {
        return false;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void destroy() {

    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }
}
